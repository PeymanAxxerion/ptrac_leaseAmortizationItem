# PTRAC Lease Amortization Loader

This service backfills and keeps the `ptrac_leaseAmortizationItem` table in sync with data fetched from Axxerion reports. It can run as a long-lived PM2 process (`pm2 start index.js --name ptamortdata --time`) or as a one-off script. It also exposes a manual loader for the `ptregion` table powered by report `PTR-REP-807`, so regional data can be refreshed from the same control plane.

## Setup

```bash
npm install
```

To expose the HTTP control API, start the process in `--serve` mode (for PM2, append the flag after `--`):

```bash
node index.js --serve
# or
pm2 restart ptamortdata -- --serve
```

By default the server listens on port `3210`; when deployed it is available via `https://ptamtdata.axxerion.net`.

## Dashboard

Visit `https://ptamtdata.axxerion.net/` once the service is running with `--serve`. Sign in with:

- Username: `admin`
- Password: `ptamortdata001`

The dashboard lets you configure the Axxerion client reference/key, report references, credentials, and batch backfill range. It also exposes buttons for the 15-minute sweep, a 7-day catchup, and the full batch load, while showing live status and a recent activity log.

Additional controls let you run the lease amortization sync as well as a PT Region load (report `PTR-REP-807`) that writes into the `ptregion` table.

## API Calls (cURL)

Replace `{refreshRef}` with your configured refresh report reference (defaults to `PTR-REP-788`) and `{batchRef}` with the batch load report reference (defaults to `PTR-REP-783`).

### Save configuration

```bash
curl -X POST 'https://ptamtdata.axxerion.net/api/config' \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "clientReference": "ptrac",
    "clientKey": "ptrac",
    "username": "axuptrac",
    "password": "secret",
    "batchReportRef": "PTR-REP-783",
    "refreshReportRef": "PTR-REP-788",
    "leaseReportRef": "PTR-REP-799",
    "regionReportRef": "PTR-REP-807",
    "batchStartIso": "2349-01-01T00:00:00",
    "batchEndIso": "2550-01-01T00:00:00"
  }'
```

Any omitted fields keep their previous values.

### Run the last 15-minute sweep

```bash
curl -X POST 'https://ptamtdata.axxerion.net/api/reports/{refreshRef}/refresh' \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "force": false,
    "verbose": true,
    "delayMs": 60000
  }'
```

- Runs the default backward sweep covering the most recent 15 minutes.
- `force=true` (query or body) retriggers even if a sweep is in progress.
- `verbose` toggles detailed logging for subsequent sweeps.
- `delayMs` overrides the per-call delay (default `60000` ms).

### Catch up N trailing days

```bash
curl -X POST 'https://ptamtdata.axxerion.net/api/reports/{refreshRef}/catchup' \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "days": 7,
    "delayMs": 60000,
    "verbose": true
  }'
```

This queues a backward sweep covering the last `days` worth of 15-minute windows. The body requires a positive `days` value; `delayMs` and `verbose` are optional.

### Trigger a batch load

```bash
curl -X POST 'https://ptamtdata.axxerion.net/api/reports/{batchRef}/batchload' \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "start": "2024-01-01T00:00:00",
    "end": "2024-12-31T23:59:59",
    "verbose": true
  }'
```

Runs the day-by-day loader between the supplied ISO timestamps. When omitted, the server falls back to the configured batch start/end values.

### Run the PT Region load

```bash
curl -X POST 'https://ptamtdata.axxerion.net/api/reports/{regionRef}/ptregion' \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "start": "2024-01-01T00:00:00",
    "end": "2024-12-31T23:59:59",
    "verbose": true
  }'
```

This issues the same per-day ingestion cycle but writes results into the `ptregion` table. Use `/api/reports/{regionRef}/ptregion/cancel` to stop an in-flight run if necessary.

### Inspect scheduler status

```bash
curl 'https://ptamtdata.axxerion.net/api/reports/{refreshRef}/status'
```

Returns the current scheduler state, including job status, last run timestamps, next scheduled execution, and preferred verbosity.

### Cancel in-flight jobs

```bash
# Cancel the active 15-minute sweep/catch-up (no error if one was already queued)
curl -X POST 'https://ptamtdata.axxerion.net/api/reports/{refreshRef}/cancel'

# Cancel the day-by-day batch loader
curl -X POST 'https://ptamtdata.axxerion.net/api/reports/{batchRef}/batchcancel'
```

Each endpoint responds with the current scheduler status so the dashboard can update immediately. If no relevant job is running you will receive HTTP `409`.

### Direct Axxerion report call (optional)

If you need to run the upstream report manually, replicate the axios POST that the loader issues. Supply the correct date range and keep the `Authorization` header value synced with your credentials.

```bash
curl -X POST 'https://{clientReference}.axxerion.us/webservices/{clientKey}/rest/functions/completereportresult' \
  -H 'Authorization: Basic {base64(username:password)}' \
  -H 'Content-Type: text/plain' \
  --data-raw '{
    "reference": "{batchRef}",
    "filterFields": ["startDate", "endDate"],
    "filterValues": ["06/25/2349 12:00 AM", "06/26/2349 12:00 AM"]
  }'
```

Replace the placeholders with the values shown on the dashboard/config endpoint. Swap the `reference` value to your refresh report reference when testing that endpoint.

## Notes

- Results are dumped under `dumps/` and failures under `results/`.
- Database connection settings are currently hard-coded in `index.js`; update them or externalize via environment variables as needed.
- After configuring PM2, run `pm2 save` to persist the process list.
