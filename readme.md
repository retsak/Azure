# CSV to Azure Monitor Logs (DCR-based) Ingestion

This repo contains PowerShell scripts to ingest CSV data into a Log Analytics custom table using the Azure Monitor Logs Ingestion API (DCR-based custom logs).

- `Ingest-CsvToLA.ps1`: Reads a CSV, converts rows to JSON, batches and posts to your DCR stream.
- `Convert-CSVtoJSON.ps1`: Optional helper to produce JSON for inspection/offline use.
- `ServiceNow_cmdb_ci_computer_sample.csv`: Sample CSV.

## What you’ll set up

1) A DCR-based custom table in your Log Analytics workspace.
2) A Data Collection Rule (DCR) with a stream mapped to that table.
3) An Entra app with client secret authorized to ingest to the DCR.
4) Environment variables or parameters to run `Ingest-CsvToLA.ps1`.

## Prerequisites

- Azure subscription and a Log Analytics workspace.
- Permissions to create a table and DCR, and to assign the "Monitoring Metrics Publisher" role on the DCR to the Service principal (Entra app).
- PowerShell 7.x (`pwsh`) on Windows or Azure Automation

## Step 1 — Create a DCR-based custom table

Portal (recommended):

- Go to Log Analytics workspace > Tables > Create > New custom log (DCR-based)
- Choose a table name (e.g., `ServiceNowSample_CL`).
- Complete the wizard. This creates the table and a DCR with a proper transform.

Notes:

- DCR custom log stream name convention is usually `<TableName>`, e.g. ServiceNowSample will result in `ServiceNowSample_CL`.
- If you data does not contain a time generated column extend it with: source
| extend TimeGenerated = todatetime(now())

## Step 2 — Confirm the DCR and stream mapping

Portal:

- Open the DCR that was created (from the table wizard) or your existing DCR.
- Check Data flows / Streams. Confirm the stream name that maps to your table (e.g., `ServiceNowSample_CL`).

Collect:

- DCR Immutable ID (looks like `dcr-xxxxxxxx...`). Portal: DCR > Properties > Immutable ID.
- Ingestion endpoint:
  - Region-level: `https://<region>.ingest.monitor.azure.com` (e.g., `https://eastus.ingest.monitor.azure.com`)
  - Or DCE endpoint: `https://<dceName>.<region>.ingest.monitor.azure.com`

## Step 3 — Create an Entra app and assign role

- Create an app registration; record its Application (client) ID and Directory (tenant) ID.
- Create a client secret; record its value.
- Grant the app the "Data Collection Rule Data Ingestor" role on the specific DCR (scope: the DCR resource). Wait a minute for RBAC to propagate.

## Step 4 — Configure environment variables (or use parameters)

You can pass parameters directly to the script, or set environment variables for convenience. The script reads these by default:

- `TENANT_ID`: Entra tenant GUID.
- `CLIENT_ID`: App registration (client) ID.
- `DCRSecret`: App registration secret.
- `IngestionEndpoint`: Region or DCE ingest host, e.g. `https://eastus.ingest.monitor.azure.com` or `https://mydce.eastus.ingest.monitor.azure.com`.
- `DCRImmutableId`: The DCR immutable ID (`dcr-...`).
- `STREAM_NAME`: The DCR stream name (e.g., `ServiceNowSample_CL`). If you provide `ServiceNowSample_CL`, the script will prefix `Custom-` automatically.

PowerShell (session-scoped):

```pwsh
$env:TENANT_ID = "<tenant-guid>"
$env:CLIENT_ID = "<app-client-id>"
$env:DCRSecret = "<app-client-secret>"
$env:IngestionEndpoint = "https://<region-or-dce>.<region>.ingest.monitor.azure.com"
$env:DCRImmutableId = "dcr-xxxxxxxxxxxxxxxxxxxxxxxxxxxx"
$env:STREAM_NAME = "ServiceNowSample_CL"  # or Custom-ServiceNowSample_CL
```

## Step 5 — Run the ingestion script

Minimal run (uses env vars when not passed as parameters):

```pwsh
pwsh -NoProfile -File "Ingest-CsvToLA.ps1" `
  -CsvPath ".\ServiceNow_cmdb_ci_computer_sample.csv"
```

Optional parameters:

- `-TimeGeneratedColumn <name>`: Column in your CSV to use as TimeGenerated (ISO 8601 or parseable). If provided and parseable, the script stamps `TimeGenerated` in UTC.
- `-MaxPostBytes <int>`: Target uncompressed JSON size per POST (default ~900KB; API limit ~1MB).

Example with options:

```pwsh
pwsh -NoProfile -File "Ingest-CsvToLA.ps1" `
  -CsvPath ".\ServiceNow_cmdb_ci_computer_sample.csv" `
  -TimeGeneratedColumn "LastUpdated" `
  -MaxPostBytes 800KB
```

## Step 6 — Verify in Log Analytics (KQL)

Use your table name (ends with `_CL` by default):

```kusto
ServiceNowSample_CL
| take 10
```

You should see records shortly after a successful POST.

## How the script treats data

- Column names are sanitized to letters/digits/underscore; leading digit gets an underscore; names longer than 45 chars are truncated.
- Values are best-effort typed:
  - `true`/`false` → booleans
  - numeric strings → numbers
  - Date/Time strings → ISO 8601 UTC strings
  - everything else → string
- If `-TimeGeneratedColumn` is set and parseable, `TimeGenerated` is stamped. Otherwise, ingestion time is used.
- Records are batched to keep the uncompressed JSON body under `-MaxPostBytes`.
- Payloads are gzipped for transport.

## Troubleshooting

- InvalidRequestPath: Ensure `STREAM_NAME` is a stream name, not a query string. Do not include `?api-version=...`. Valid examples: `Custom-MyTable_CL` or `MyTable_CL` (the script adds `Custom-`). The final URL should look like:
  `https://<endpoint>/dataCollectionRules/<dcr-id>/streams/Custom-MyTable_CL?api-version=2023-01-01`
- 401 Unauthorized: Check tenant/client/secret values. Ensure you’re using the v2.0 token endpoint and scope `https://monitor.azure.com/.default` (the script does this).
- 403 Forbidden: The app must have the role "Data Collection Rule Data Ingestor" on the DCR (correct scope). Wait for RBAC to propagate.
- 404 Not Found: Wrong DCR Immutable ID or wrong ingestion endpoint/region/DCE.
- 413/Request too large: Reduce `-MaxPostBytes` (uncompressed JSON target) or split the CSV.
- 429/5xx: The script retries with exponential backoff automatically.
- Unexpected columns: The DCR transform must map incoming JSON to the table schema. Use the custom log (DCR-based) wizard or adjust the DCR transform as needed.

## Optional: CLI snippets to discover values

```pwsh
# DCR immutable ID
az monitor data-collection rule show -g <rg> -n <dcrName> --query immutableId -o tsv

# Ingestion endpoint (region)
#   https://<region>.ingest.monitor.azure.com
# Ingestion endpoint (DCE)
#   https://<dceName>.<region>.ingest.monitor.azure.com
```

## Files in this repo

- `Ingest-CsvToLA.ps1` — Ingests CSV rows to your DCR stream with batching, gzip, and retries.
- `Convert-CSVtoJSON.ps1` — Converts CSV rows to JSON (no ingestion) for inspection.
- `ServiceNow_cmdb_ci_computer_sample.csv` — Sample input.
- `ServiceNow_cmdb_ci_computer_sample.json` — Sample JSON output (for reference).

## Tips

- Keep the table schema aligned with your CSV fields. If you change CSV headers, update the DCR transform (or re-run the table wizard) accordingly.
- Prefer UTC timestamps in your CSV when using `-TimeGeneratedColumn`.
- For very large CSVs, consider splitting files or reducing `-MaxPostBytes`.
