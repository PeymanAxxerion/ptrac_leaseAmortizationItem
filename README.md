# PTRAC Lease Amortization Loader

This service backfills and keeps the `ptrac_leaseAmortizationItem` table in sync with data fetched from Axxerion reports. It can run as a long-lived PM2 process (`pm2 start index.js --name ptamortdata --time`) or as a one-off script.

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

## API Calls (cURL)

### Run the last 15-minute sweep

```bash
curl -X POST 'https://ptamtdata.axxerion.net/api/reports/PTR-REP-788/refresh' \
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
curl -X POST 'https://ptamtdata.axxerion.net/api/reports/PTR-REP-788/catchup' \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "days": 7,
    "delayMs": 60000,
    "verbose": true
  }'
```

This queues a backward sweep covering the last `days` worth of 15-minute windows. The body requires a positive `days` value; `delayMs` and `verbose` are optional.

### Inspect scheduler status

```bash
curl 'https://ptamtdata.axxerion.net/api/reports/PTR-REP-788/status'
```

Returns the current scheduler state, including job status, last run timestamps, next scheduled execution, and preferred verbosity.

### Direct Axxerion report call (optional)

If you need to run the upstream report manually, replicate the axios POST that the loader issues. Supply the correct date range and keep the `Authorization` header value synced with your credentials.

```bash
curl -X POST 'https://ptrac.axxerion.us/webservices/ptrac/rest/functions/completereportresult' \
  -H 'Authorization: Basic YXh1cHRyYWM6UXJlcG9pITU1' \
  -H 'Content-Type: text/plain' \
  --data-raw '{
    "reference": "PTR-REP-783",
    "filterFields": ["startDate", "endDate"],
    "filterValues": ["06/25/2349 12:00 AM", "06/26/2349 12:00 AM"]
  }'
```

Replace `filterValues` with the desired window and swap `"reference"` for `"PTR-REP-788"` when hitting that report.

## Notes

- Results are dumped under `dumps/` and failures under `results/`.
- Database connection settings are currently hard-coded in `index.js`; update them or externalize via environment variables as needed.
- After configuring PM2, run `pm2 save` to persist the process list.
