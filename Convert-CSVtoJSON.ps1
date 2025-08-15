param(
    [Parameter(Position=0)]
    [string]$InputPath = (Join-Path -Path (Get-Location) -ChildPath 'ServiceNow_cmdb_ci_computer_sample.csv'),

    [Parameter(Position=1)]
    [string]$OutputPath,

    [int]$Depth = 32,
    [switch]$AutoType,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Convert-Field {
    param([Object]$Value)

    if ($null -eq $Value) { return $null }
    if ($Value -isnot [string]) { return $Value }
    $s = $Value.Trim()
    if ($s.Length -eq 0) { return $null }

    # Booleans
    if ($s -match '^(?i:true|false)$') { return [bool]::Parse($s) }

    # Integers
    $i64 = 0L
    if ([long]::TryParse($s, [ref]$i64)) { return $i64 }

    # Floats
    $dbl = 0.0
    if ([double]::TryParse($s, [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$dbl)) {
        return $dbl
    }

    # Date/Time
    $dt = [datetime]::MinValue
    if ([datetime]::TryParse($s, [ref]$dt)) { return $dt }

    return $s
}

# Resolve input
try {
    $resolvedInput = (Resolve-Path -LiteralPath $InputPath).Path
} catch {
    throw "Input CSV not found: $InputPath"
}

# Default output to same path with .json
if (-not $OutputPath) {
    $OutputPath = [System.IO.Path]::ChangeExtension($resolvedInput, '.json')
}

# Ensure output directory exists
$outDir = Split-Path -Parent $OutputPath
if ($outDir -and -not (Test-Path -LiteralPath $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}

# Prevent accidental overwrite
if ((Test-Path -LiteralPath $OutputPath) -and -not $Force) {
    throw "Output file exists: $OutputPath (use -Force to overwrite)"
}

# Import, optionally auto-type fields, then convert to JSON
$rows = Import-Csv -LiteralPath $resolvedInput

if ($AutoType) {
    $rows = $rows | ForEach-Object {
        $ordered = [ordered]@{}
        foreach ($p in $_.PSObject.Properties) {
            $ordered[$p.Name] = Convert-Field -Value $p.Value
        }
        [pscustomobject]$ordered
    }
}

$json = $rows | ConvertTo-Json -Depth $Depth

# Write UTF-8 (no BOM)
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($OutputPath, $json, $utf8NoBom)