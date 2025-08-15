<#
.SYNOPSIS
  Ingest a CSV file into a DCR-based custom table via the Azure Monitor Logs Ingestion API.

.PREREQS
  1) Create the table (Portal: Log Analytics workspace > Tables > Create > New custom log (DCR-based))
  2) Ensure a DCR exists with a stream that maps to that table.
  3) Create an Entra app (Client ID/Secret) and grant it Data Collection Rule Data Ingestor on the DCR.

.PARAMS
  -TenantId            Entra tenant GUID.
  -ClientId            App registration (client) ID.
  -ClientSecret        App registration secret.
  -IngestionEndpoint   Base ingest host for your DCR or DCE, e.g. https://<region>.ingest.monitor.azure.com or https://<dceName>.<region>.ingest.monitor.azure.com
  -DcrImmutableId      The DCR immutable ID (starts with 'dcr-...').
  -StreamName          DCR stream to post to (e.g., Custom-MyTable).
  -CsvPath             Path to CSV.
  -TimeGeneratedColumn Optional: column in CSV to use for TimeGenerated (ISO8601 assumed or parseable).
  -MaxPostBytes        Target uncompressed JSON size per POST; default 900KB (API max ~1MB).
#>

param(
  [Parameter()][ValidateNotNullOrEmpty()] [string] $TenantId          = $env:TENANT_ID,
  [Parameter()][ValidateNotNullOrEmpty()] [string] $ClientId          = $env:CLIENT_ID,
  [Parameter()][ValidateNotNullOrEmpty()] [string] $ClientSecret      = $env:DCRSecret,
  [Parameter()][ValidateNotNullOrEmpty()] [string] $IngestionEndpoint = $env:IngestionEndpoint,
  [Parameter()][ValidateNotNullOrEmpty()] [string] $DcrImmutableId    = $env:DCRImmutableId,
  [Parameter()][ValidateNotNullOrEmpty()] [string] $StreamName        = $env:STREAM_NAME,
  [Parameter(Mandatory=$true)] [string] $CsvPath,
  [datetime] $ScriptRunTimeUtc = (Get-Date).ToUniversalTime(),
  [int] $MaxPostBytes = 900KB
)

# -------------------- Helpers --------------------

function Get-OAuthToken {
  param(
    [string]$TenantId, [string]$ClientId, [string]$ClientSecret,
    [string]$Scope = 'https://monitor.azure.com/.default'
  )
  $tokenUri = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
  $body = @{
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = $Scope
    grant_type    = 'client_credentials'
  }
  (Invoke-RestMethod -Method Post -Uri $tokenUri -Body $body -ContentType 'application/x-www-form-urlencoded').access_token
}

function ConvertTo-SafeName { param([string]$n)
  $s = ($n -replace '[^A-Za-z0-9_]', '_')
  if ($s -match '^[0-9]') { $s = "_$s" }
  if ([string]::IsNullOrWhiteSpace($s)) { $s = 'col' }
  if ($s.Length -gt 45) { $s = $s.Substring(0,45) } # table limits are tighter now
  $s
}

function ConvertTo-StrongTypes {
  param([hashtable]$Record)
  $out = [ordered]@{}
  foreach ($kv in $Record.GetEnumerator()) {
    $k,$v = $kv.Key,$kv.Value
    if ($null -eq $v -or $v -eq '') { $out[$k] = $null; continue }

    # boolean?
    if ($v -is [string]) {
      $l = $v.ToLowerInvariant()
      if ($l -eq 'true') { $out[$k] = $true; continue }
      if ($l -eq 'false'){ $out[$k] = $false; continue }
    }

    # number?
    [double]$num = 0
    if ([double]::TryParse($v, [Globalization.NumberStyles]::Any,
        [Globalization.CultureInfo]::InvariantCulture, [ref]$num)) {
      $out[$k] = $num; continue
    }

    # datetime?
    [datetime]$dt = 0
    if ([datetime]::TryParse($v, [ref]$dt)) {
      $out[$k] = $dt.ToUniversalTime().ToString('o'); continue
    }

    $out[$k] = $v
  }
  $out
}

function New-JsonLinesFromCsvRow {
  param([pscustomobject]$Row, [string]$TimeGeneratedColumn)

  $h = @{}
  foreach ($p in $Row.PSObject.Properties) {
  $h[(ConvertTo-SafeName $p.Name)] = $p.Value
  }

  if ($TimeGeneratedColumn -and ($Row.PSObject.Properties.Name -contains $TimeGeneratedColumn)) {
    $tg = $Row.$TimeGeneratedColumn
    [datetime]$dt = 0
    if ([datetime]::TryParse($tg, [ref]$dt)) {
      $h['TimeGenerated'] = $dt.ToUniversalTime().ToString('o')
    }
  }

  ConvertTo-StrongTypes -Record $h
}

function Compress-GzipUtf8 {
  param([string]$Text)
  $ms = New-Object System.IO.MemoryStream
  $gz = New-Object System.IO.Compression.GzipStream($ms, [IO.Compression.CompressionLevel]::SmallestSize, $true)
  $sw = New-Object System.IO.StreamWriter($gz, [Text.Encoding]::UTF8)
  $sw.Write($Text); $sw.Close()
  $bytes = $ms.ToArray(); $ms.Dispose()
  ,$bytes
}

function Invoke-LogIngestion {
  param(
    [byte[]]$BodyBytesGz,
    [string]$Token,
    [string]$Uri
  )
  $headers = @{
    Authorization     = "Bearer $Token"
    'Content-Type'    = 'application/json'
    'Content-Encoding'= 'gzip'
  }

  $maxAttempts = 5
  $delay = 1
  for ($i=1;$i -le $maxAttempts;$i++) {
    try {
  Invoke-RestMethod -Method Post -Uri $Uri -Headers $headers -Body $BodyBytesGz | Out-Null
      return $true
    } catch {
      $status = $_.Exception.Response.StatusCode.value__
      if ($status -in 429,500,502,503,504) {
        Start-Sleep -Seconds $delay; $delay = [math]::Min(30, [int]([math]::Pow(2,$i)))
      } else {
        throw $_
      }
    }
  }
  $false
}

# -------------------- Main --------------------

if (-not (Test-Path -LiteralPath $CsvPath)) { throw "CSV not found: $CsvPath" }

$token = Get-OAuthToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret

# Normalize endpoint (no trailing slash)
$base = $IngestionEndpoint.TrimEnd('/')

# Validate StreamName early (common source of InvalidRequestPath)
if ([string]::IsNullOrWhiteSpace($StreamName)) {
  throw "StreamName is empty. Provide -StreamName (e.g., 'Custom-MyTable') or set $env:STREAM_NAME."
}
if ($StreamName -match 'version=') {
  throw "StreamName looks wrong ('$StreamName'). Do not include '?api-version=...'; pass only the DCR stream name like 'Custom-MyTable'."
}

# Ensure expected stream prefix for DCR custom tables
if (-not $StreamName.StartsWith('Custom-')) {
  Write-Host "StreamName does not start with 'Custom-'. Using 'Custom-$StreamName' for DCR custom table ingest." -ForegroundColor Yellow
  $StreamName = "Custom-$StreamName"
}

# Stable API version for Logs Ingestion. Build URI with UriBuilder to ensure the query is preserved.
$builder = [System.UriBuilder]$base
$builder.Path = ($builder.Path.TrimEnd('/') + "/dataCollectionRules/$DcrImmutableId/streams/$StreamName")
$builder.Query = 'api-version=2023-01-01'
$uri = $builder.Uri.AbsoluteUri

$rows = Import-Csv -LiteralPath $CsvPath
Write-Host "Loaded $($rows.Count) CSV rows. Posting to $uri" -ForegroundColor Cyan

# Build and send batches under ~1MB uncompressed
$buffer = New-Object System.Collections.Generic.List[object]
$total = 0

foreach ($r in $rows) {
  $obj = New-JsonLinesFromCsvRow -Row $r -TimeGeneratedColumn $TimeGeneratedColumn
  $buffer.Add($obj) | Out-Null

  $jsonTest = ($buffer | ConvertTo-Json -Depth 12 -Compress)
  $bytes = [Text.Encoding]::UTF8.GetByteCount($jsonTest)

  if ($bytes -ge $MaxPostBytes) {
    # remove last, send current batch
    $buffer.RemoveAt($buffer.Count-1)
    if ($buffer.Count -gt 0) {
      $json = ($buffer | ConvertTo-Json -Depth 12 -Compress)
      $gz = Compress-GzipUtf8 -Text $json
      Invoke-LogIngestion -BodyBytesGz $gz -Token $token -Uri $uri | Out-Null
      $total += $buffer.Count
      Write-Host "Sent $($buffer.Count) records" -ForegroundColor Green
      $buffer.Clear()
    }
    # start new batch with the current record
    $buffer.Add($obj) | Out-Null
  }
}

if ($buffer.Count -gt 0) {
  $json = ($buffer | ConvertTo-Json -Depth 12 -Compress)
  $gz = Compress-GzipUtf8 -Text $json
  Invoke-LogIngestion -BodyBytesGz $gz -Token $token -Uri $uri | Out-Null
  $total += $buffer.Count
  Write-Host "Sent $($buffer.Count) records" -ForegroundColor Green
}

Write-Host "Done. Total records sent: $total" -ForegroundColor Cyan
