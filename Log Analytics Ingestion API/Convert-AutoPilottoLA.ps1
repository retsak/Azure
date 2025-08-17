param(
  [Parameter()][ValidateNotNullOrEmpty()] [string] $TenantId          = $env:TENANT_ID,
  [Parameter()][ValidateNotNullOrEmpty()] [string] $ClientId          = $env:CLIENT_ID,
  [Parameter()][ValidateNotNullOrEmpty()] [string] $ClientSecret      = $env:DCRSecret,
  [Parameter()][ValidateNotNullOrEmpty()] [string] $IngestionEndpoint = $env:IngestionEndpoint,
  [Parameter()][ValidateNotNullOrEmpty()] [string] $DcrImmutableId    = $env:DCRImmutableId_Autopilot,
  [Parameter()][ValidateNotNullOrEmpty()] [string] $StreamName        = $env:STREAM_NAME_AUTOPILOT,
  [string] $DcrResourceId = $env:DCR_RESOURCE_ID_AUTOPILOT,
  [switch] $PreflightOnly,
  [Nullable[datetime]] $ScriptRunTimeUtc = $null,
  [int] $MaxPostBytes = 900KB
)

#Connect-AzAccount -Identity | Out-Null
#$token = (Get-AzAccessToken -ResourceTypeName MSGraph -AsSecureString).token
#Connect-MgGraph -AccessToken $token

# Enable TLS 1.2 support 
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# If not provided, set ScriptRunTimeUtc now to avoid param metadata issues
if (-not $ScriptRunTimeUtc) { $ScriptRunTimeUtc = (Get-Date).ToUniversalTime() }

# Validate required configuration early for clearer errors
if ([string]::IsNullOrWhiteSpace($TenantId))          { throw "TENANT_ID missing. Set -TenantId or $env:TENANT_ID." }
if ([string]::IsNullOrWhiteSpace($ClientId))          { throw "CLIENT_ID (ingestor app) missing. Set -ClientId or $env:CLIENT_ID." }
if ([string]::IsNullOrWhiteSpace($ClientSecret))      { throw "DCRSecret (ingestor app secret) missing. Set -ClientSecret or $env:DCRSecret." }
if ([string]::IsNullOrWhiteSpace($IngestionEndpoint)) { throw "IngestionEndpoint missing. Set -IngestionEndpoint or $env:IngestionEndpoint (e.g., https://<region>.ingest.monitor.azure.com)." }
if ([string]::IsNullOrWhiteSpace($DcrImmutableId))    { throw "DCRImmutableId_Autopilot missing. Set -DcrImmutableId or $env:DCRImmutableId_Autopilot (starts with 'dcr-')." }
if ([string]::IsNullOrWhiteSpace($StreamName))        { throw "STREAM_NAME_AUTOPILOT missing. Set -StreamName or `$env:STREAM_NAME_AUTOPILOT (e.g., Custom-AutoPilotDevices)." }

# Preflight: summarize config, build URI, and test token acquisition
function Show-Preflight {
  param(
    [string] $TenantId,
    [string] $ClientId,
    [string] $ClientSecret,
    [string] $IngestionEndpoint,
    [string] $DcrImmutableId,
    [string] $StreamName,
    [string] $DcrResourceId
  )

  $base = $IngestionEndpoint.TrimEnd('/')
  $effStream = if ($StreamName.StartsWith('Custom-')) { $StreamName } else { "Custom-$StreamName" }
  $addedSuffix = $false
  if (-not $effStream.ToLower().EndsWith('_cl')) { $effStream = "$effStream`_CL"; $addedSuffix = $true }

  $builder = [System.UriBuilder]$base
  $builder.Path = ($builder.Path.TrimEnd('/') + "/dataCollectionRules/$DcrImmutableId/streams/$effStream")
  $builder.Query = 'api-version=2023-01-01'
  $uriPreview = $builder.Uri.AbsoluteUri

  $tidShort = if ($TenantId.Length -gt 8) { $TenantId.Substring(0,8) + '…' } else { $TenantId }

  Write-Host "--- Preflight ---" -ForegroundColor Cyan
  Write-Host ("TenantId:            {0}" -f $tidShort)
  Write-Host ("ClientId (ingestor): {0}" -f $ClientId)
  Write-Host ("IngestionEndpoint:   {0}" -f $IngestionEndpoint)
  Write-Host ("DCR Immutable Id:    {0}" -f $DcrImmutableId)
  Write-Host ("Stream (effective):  {0}" -f $effStream)
  if ($addedSuffix) { Write-Host "Note: '_CL' suffix appended to match DCR custom log convention." -ForegroundColor Yellow }
  if (-not [string]::IsNullOrWhiteSpace($DcrResourceId)) {
    Write-Host ("DCR Resource Id:     {0}" -f $DcrResourceId)
    Write-Host "RBAC scope to assign: Monitoring Data Contributor at the DCR resource id above" -ForegroundColor Yellow
  } else {
    Write-Host "Tip: Set DCR_RESOURCE_ID_AUTOPILOT to print the exact RBAC scope (ARM id of the DCR)." -ForegroundColor Yellow
    Write-Host "Pattern: /subscriptions/<subId>/resourceGroups/<rg>/providers/Microsoft.Insights/dataCollectionRules/<dcrName>" -ForegroundColor Yellow
  }
  if ($IngestionEndpoint -notlike "https://*.ingest.monitor.azure.com*") {
    Write-Host "Warning: IngestionEndpoint doesn't look like an ingest host. Expected https://<region>.ingest.monitor.azure.com or https://<dce>.<region>.ingest.monitor.azure.com" -ForegroundColor Yellow
  }
  Write-Host ("Ingestion URI preview: {0}" -f $uriPreview)

  try {
    $tok = Get-OAuthToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
    if ($tok) { Write-Host "Token acquisition: OK" -ForegroundColor Green }
    else { Write-Host "Token acquisition: FAILED (empty token)" -ForegroundColor Red }
  } catch {
    Write-Host "Token acquisition failed: $($_.Exception.Message)" -ForegroundColor Red
  }
  Write-Host "--- End Preflight ---" -ForegroundColor Cyan
}

Show-Preflight -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret -IngestionEndpoint $IngestionEndpoint -DcrImmutableId $DcrImmutableId -StreamName $StreamName -DcrResourceId $DcrResourceId
if ($PreflightOnly) { return }

# Connect to Microsoft Graph API - User
Connect-MgGraph -TenantId $env:TENANT_ID -ClientId $env:CLIENT_ID_USER -Verbose -NoWelcome
$autoPilotDevices = Get-MgBetaDeviceManagementWindowsAutopilotDeviceIdentity -All | Select-Object Id, ManagedDeviceId, Manufacturer, Model, SerialNumber, AzureActiveDirectoryDeviceId, GroupTag
#Disconnect-MgGraph
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

# Build a JSON-ready hashtable from an arbitrary PSObject (AutoPilot device),
# applying safe names and strong typing, and adding TimeGenerated.
function New-JsonFromObject {
  param(
    [psobject] $Object,
    [datetime] $TimeGenerated = (Get-Date).ToUniversalTime()
  )
  $h = @{}
  foreach ($p in $Object.PSObject.Properties) {
    $h[(ConvertTo-SafeName $p.Name)] = $p.Value
  }
  # Add TimeGenerated if not already present
  if (-not $h.ContainsKey('TimeGenerated')) {
    $h['TimeGenerated'] = $TimeGenerated.ToUniversalTime().ToString('o')
  }
  ConvertTo-StrongTypes -Record $h
}

# -------------------- Main: Ingest $autoPilotDevices --------------------

# Acquire OAuth token for Logs Ingestion
$token = Get-OAuthToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret

# Normalize endpoint (no trailing slash)
$base = $IngestionEndpoint.TrimEnd('/')

# Validate StreamName and ensure Custom- prefix for DCR custom tables
if ([string]::IsNullOrWhiteSpace($StreamName)) {
  throw "StreamName is empty. Provide -StreamName (e.g., 'Custom-MyTable') or set $env:STREAM_NAME."
}
if ($StreamName -match 'version=') {
  throw "StreamName looks wrong ('$StreamName'). Do not include '?api-version=...'; pass only the DCR stream name like 'Custom-MyTable'."
}
if (-not $StreamName.StartsWith('Custom-')) {
  Write-Host "StreamName does not start with 'Custom-'. Using 'Custom-$StreamName' for DCR custom table ingest." -ForegroundColor Yellow
  $StreamName = "Custom-$StreamName"
}
# Append _CL if missing for DCR custom log tables
if (-not $StreamName.ToLower().EndsWith('_cl')) {
  Write-Host "StreamName missing '_CL' suffix. Using '$StreamName`_CL' to match DCR custom log convention." -ForegroundColor Yellow
  $StreamName = "$StreamName`_CL"
}

# Build URI with stable API version
$builder = [System.UriBuilder]$base
$builder.Path = ($builder.Path.TrimEnd('/') + "/dataCollectionRules/$DcrImmutableId/streams/$StreamName")
$builder.Query = 'api-version=2023-01-01'
$uri = $builder.Uri.AbsoluteUri

Write-Host "Loaded $($autoPilotDevices.Count) Autopilot devices. Posting to $uri" -ForegroundColor Cyan

# Batch and send under ~1MB (uncompressed) per POST
$buffer = New-Object System.Collections.Generic.List[object]
$total = 0

foreach ($dev in $autoPilotDevices) {
  $obj = New-JsonFromObject -Object $dev -TimeGenerated $ScriptRunTimeUtc
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

