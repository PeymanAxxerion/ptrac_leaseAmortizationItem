import axios from "axios";
import { Client } from "pg";
import { DateTime } from "luxon";
import { CronJob } from "cron";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import fs from "fs";
import path from "path";

/** ===== Config ===== */
const AX_URL = "https://ptrac.axxerion.us/webservices/ptrac/rest/functions/completereportresult";
const AX_AUTH_BASIC = "Basic YXh1cHRyYWM6UXJlcG9pITU1";
const AX_REFERENCE = "PTR-REP-783";
const TZ = "America/Los_Angeles";

const PG = {
  host: "195.35.11.138",
  port: 5440,
  user: "postgres",
  password: "AXU001",
  database: "axdb",
  ssl: false
};

const DEST_TABLE = "public.ptrac_leaseAmortizationItem";
const FAILURE_DIR = path.join(process.cwd(), "results");
const FAILURE_JSON_PATH = path.join(FAILURE_DIR, "failed-windows.json");
const FAILURE_HTML_PATH = path.join(FAILURE_DIR, "failed-windows.html");

const failureRecords = [];
const DAILY_BATCH_SIZE = 90;
const DAILY_BATCH_DELAY_MS = 1500;
const DEFAULT_BACKFILL_START_ISO = "2349-01-01T00:00:00";
const DEFAULT_BACKFILL_END_ISO = "2550-01-01T00:00:00";

/** ===== CLI ===== */
const argv = yargs(hideBin(process.argv))
  .option("test", { type: "boolean", default: false, desc: "Run a specific test window and write to DB" })
  .option("verbose", { type: "boolean", default: true })
  .option("log-http", { type: "boolean", default: true })
  .option("dump", { type: "boolean", default: true })
  .option("field-start", { type: "string", default: "startDate" })
  .option("field-end", { type: "string", default: "endDate" })
  .option("from", { type: "string", desc: "Override backfill start (YYYY-MM-DD)" })
  .option("to", { type: "string", desc: "Override backfill end (YYYY-MM-DD)" })
  .parse();

/** ===== Helpers ===== */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ensureDir = (p) => { if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true }); };

function fmtAxxerion(dt) {
  return DateTime.fromJSDate(dt instanceof Date ? dt : new Date(dt), { zone: TZ })
    .toFormat("MM/dd/yyyy hh:mm a"); // e.g. 02/07/2000 12:00 AM
}

function parseMaybeDate(v) {
  if (!v) return null;
  let d = DateTime.fromISO(String(v), { zone: TZ });
  if (d.isValid) return d;
  const formats = ["MM/dd/yyyy hh:mm a","MM/dd/yyyy HH:mm","MM/dd/yyyy","yyyy-LL-dd HH:mm:ss","yyyy-LL-dd"];
  for (const f of formats) {
    d = DateTime.fromFormat(String(v), f, { zone: TZ });
    if (d.isValid) return d;
  }
  return null;
}

function parseRangeBoundary(label, rawValue, fallbackIso) {
  const candidate = rawValue != null && String(rawValue).trim() !== "" ? String(rawValue).trim() : fallbackIso;
  const dt = parseMaybeDate(candidate);
  if (!dt) throw new Error(`Invalid ${label} date: ${candidate}`);
  return dt;
}

function extractKey(rec) {
  if (!rec || typeof rec !== "object") return null;
  if (rec.id != null && String(rec.id).trim() !== "") return String(rec.id);
  if (rec.objectid != null && String(rec.objectid).trim() !== "") return String(rec.objectid);
  if (rec.ObjectId != null && String(rec.ObjectId).trim() !== "") return String(rec.ObjectId);
  return null; // strictly require a key
}

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function recordFailures(entries) {
  if (!entries?.length) return;
  let changed = false;

  for (const entry of entries) {
    const start = entry.start;
    const end = entry.end;
    const message = entry.message;
    const raw = entry.raw;
    const occurredAt = entry.occurredAt ?? new Date().toISOString();

    const existing = failureRecords.find((r) => r.start === start && r.end === end);
    if (existing) {
      existing.message = message;
      existing.lastOccurredAt = occurredAt;
      existing.count += 1;
      if (raw !== undefined) existing.raw = raw;
    } else {
      failureRecords.push({
        start,
        end,
        message,
        raw,
        firstOccurredAt: occurredAt,
        lastOccurredAt: occurredAt,
        count: 1
      });
    }
    changed = true;
  }

  if (changed) writeFailureReport();
}

function writeFailureReport() {
  ensureDir(FAILURE_DIR);
  const sorted = failureRecords
    .slice()
    .sort((a, b) => a.start.localeCompare(b.start) || a.end.localeCompare(b.end));

  fs.writeFileSync(FAILURE_JSON_PATH, JSON.stringify(sorted, null, 2));

  const rows = sorted
    .map(
      (r, idx) => `
        <tr>
          <td>${idx + 1}</td>
          <td>${escapeHtml(r.start)}</td>
          <td>${escapeHtml(r.end)}</td>
          <td>${escapeHtml(r.message)}</td>
          <td>${escapeHtml(r.count)}</td>
          <td>${escapeHtml(r.firstOccurredAt)}</td>
          <td>${escapeHtml(r.lastOccurredAt)}</td>
        </tr>`
    )
    .join("\n");

  const html = `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>PTRAC Loader – Failed Windows</title>
    <style>
      :root { color-scheme: light dark; font-family: system-ui, -apple-system, Segoe UI, sans-serif; }
      body { margin: 0; padding: 2rem; background: #0f172a; color: #e2e8f0; }
      h1 { margin-top: 0; }
      table { width: 100%; border-collapse: collapse; margin-top: 1.5rem; font-size: 0.9rem; }
      th, td { border: 1px solid rgba(148, 163, 184, 0.4); padding: 0.6rem; text-align: left; vertical-align: top; }
      th { background: rgba(30, 41, 59, 0.8); }
      tr:nth-child(even) { background: rgba(15, 23, 42, 0.6); }
      tr:nth-child(odd) { background: rgba(30, 41, 59, 0.6); }
      .hint { font-size: 0.85rem; color: #94a3b8; }
      code { font-family: SFMono-Regular, Consolas, Liberation Mono, monospace; }
    </style>
  </head>
  <body>
    <h1>Failed API Windows</h1>
    <p class="hint">Report generated ${escapeHtml(new Date().toISOString())}. Data source: ${escapeHtml(
      FAILURE_JSON_PATH
    )}</p>
    ${
      rows.trim()
        ? `<table>
        <thead>
          <tr>
            <th>#</th>
            <th>Start</th>
            <th>End</th>
            <th>Error</th>
            <th>Attempts</th>
            <th>First Occurred</th>
            <th>Last Occurred</th>
          </tr>
        </thead>
        <tbody>
          ${rows}
        </tbody>
      </table>`
        : `<p>No failures recorded.</p>`
    }
  </body>
</html>`;

  fs.writeFileSync(FAILURE_HTML_PATH, html);
}

async function ensureTable(pg) {
  await pg.query(`
    CREATE TABLE IF NOT EXISTS ${DEST_TABLE}(
      id text PRIMARY KEY,
      created_at timestamptz,
      updated_at timestamptz,
      data jsonb NOT NULL
    );
  `);
}

/** ---- Robust JSON parsing helpers ---- */

// Try strict JSON.parse first; if it fails, try to extract {"data":[...]} from a text blob.
function parseAxxerionPayload(rawText) {
  const txt = String(rawText ?? "").trim();

  // Attempt strict parse
  try {
    const j = JSON.parse(txt);
    return j;
  } catch (_) {}

  // Remove BOM if present
  const noBom = txt.replace(/^\uFEFF/, "");

  // If text contains a JSON object; slice from first { to last }
  const firstBrace = noBom.indexOf("{");
  const lastBrace = noBom.lastIndexOf("}");
  if (firstBrace !== -1 && lastBrace > firstBrace) {
    const candidate = noBom.slice(firstBrace, lastBrace + 1);
    try {
      const j2 = JSON.parse(candidate);
      return j2;
    } catch (_) {}
  }

  // Plan B: regex the "data":[ ... ] array and wrap
  const dataMatch = noBom.match(/"data"\s*:\s*(\[[\s\S]*\])/);
  if (dataMatch) {
    const arrStr = dataMatch[1];
    try {
      const arr = JSON.parse(arrStr);
      return { data: arr };
    } catch (_) {}
  }

  // Nothing worked
  return null;
}

async function callAxxerion(startDt, endDt) {
  const body = {
    reference: AX_REFERENCE,
    filterFields: [argv["field-start"], argv["field-end"]],
    filterValues: [fmtAxxerion(startDt), fmtAxxerion(endDt)]
  };

  const payload = JSON.stringify(body);

  if (argv["log-http"]) {
    console.log("HTTP POST", AX_URL);
    console.log("Headers:", { Authorization: "(redacted)", "Content-Type": "text/plain" });
    console.log("Body:", payload);
  }

  // Force axios to give us raw text, do our own parsing
  const resp = await axios.post(AX_URL, payload, {
    headers: { Authorization: AX_AUTH_BASIC, "Content-Type": "text/plain" },
    responseType: "text",
    transformResponse: [(d) => d], // no auto JSON
    timeout: 1800000,
    validateStatus: () => true
  });

  if (argv["log-http"]) {
    console.log("-> Status:", resp.status, resp.statusText);
    console.log("-> Resp first 400 chars:", String(resp.data).slice(0, 400));
  }

  return resp.data; // raw text
}

function normalizeRecords(rawText) {
  const parsed = parseAxxerionPayload(rawText);
  if (!parsed) return { rows: [], raw: rawText, parsed: null };

  let rows = [];
  if (Array.isArray(parsed)) rows = parsed;
  else if (parsed?.rows && Array.isArray(parsed.rows)) rows = parsed.rows;
  else if (parsed?.result && Array.isArray(parsed.result)) rows = parsed.result;
  else if (parsed?.data && Array.isArray(parsed.data)) rows = parsed.data;
  else if (typeof parsed === "object") rows = [parsed];

  return { rows, raw: rawText, parsed };
}

/** Insert/upsert 1-by-1, keyed by id/objectid; skip if no key. No writes on empty window. */
async function upsertByKey(pg, records, winStart, winEnd, verbose) {
  const sql = `
    INSERT INTO ${DEST_TABLE}(id, created_at, updated_at, data)
    VALUES ($1,$2,$3,$4)
    ON CONFLICT (id) DO UPDATE
      SET updated_at = EXCLUDED.updated_at,
          data = EXCLUDED.data`;

  let loaded = 0;
  let skippedNoKey = 0;
  for (let i = 0; i < records.length; i++) {
    const r = records[i] ?? {};
    const key = extractKey(r);
    if (!key) {
      skippedNoKey++;
      if (verbose) console.log(`   skipping record ${i + 1}/${records.length}: missing id/objectid field`);
      continue;
    }

    const createdCand = r.created_at ?? r.createdAt ?? r.createDate ?? r.CreatedDate ?? r.created ?? r.startDate;
    const updatedCand = r.updated_at ?? r.updatedAt ?? r.updateDate ?? r.UpdatedDate ?? r.updated ?? r.lastModifiedDate;
    const created = parseMaybeDate(createdCand)?.toJSDate() ?? winStart.toJSDate();
    const updated = parseMaybeDate(updatedCand)?.toJSDate() ?? winEnd.toJSDate();

    await pg.query({ text: sql, values: [key, created, updated, JSON.stringify(r)] });
    loaded++;
    if (verbose && (loaded % 200 === 0)) console.log(`   upserted ${loaded}/${records.length} ...`);
  }
  return { loaded, skippedNoKey };
}

async function processWindow(pg, startDt, endDt, verbose) {
  console.log(`-> Fetch ${startDt.setZone(TZ).toISO()} .. ${endDt.setZone(TZ).toISO()}`);

  const rawText = await callAxxerion(startDt.toJSDate(), endDt.toJSDate());
  const { rows, raw, parsed } = normalizeRecords(rawText);

  if (argv.dump) {
    const dir = path.join(process.cwd(), "dumps");
    ensureDir(dir);
    const f = path.join(
      dir,
      `${startDt.toISO({ suppressMilliseconds: true })}__${endDt.toISO({ suppressMilliseconds: true })}.json`
        .replace(/[:]/g, "-")
    );
    // Dump the raw text as-is so we can inspect any non-JSON wrappers
    fs.writeFileSync(f, typeof raw === "string" ? raw : String(raw));
    if (verbose) console.log("   wrote raw dump:", f);
  }

  console.log(`   records_from_api=${rows.length}`);
  if (rows.length) {
    console.log("   sample_keys:", Object.keys(rows[0]).slice(0, 20).join(", "));
    const { loaded, skippedNoKey } = await upsertByKey(pg, rows, startDt, endDt, verbose);
    if (skippedNoKey) {
      console.log(`   loaded_to_db=${loaded} (skipped_missing_key=${skippedNoKey})`);
    } else {
      console.log(`   loaded_to_db=${loaded}`);
    }
  } else {
    // If parsed is null but raw contains "data":[ ... ] we should have caught it;
    // log a hint showing the first 200 chars to help diagnose further edge cases.
    if (!parsed) {
      console.log("   WARN: could not parse payload; first 200 chars:");
      console.log(String(raw).slice(0, 200));
    }
    console.log("   (no DB writes; empty window)");
  }
}

async function processDailyBatch(pg, batch, batchIndex, verbose, label = "") {
  const labelPrefix = label ? `[${label}] ` : "";
  console.log(`${labelPrefix}Processing batch ${batchIndex} (${batch.length} day window(s))...`);
  const results = await Promise.allSettled(
    batch.map(({ start, end }) =>
      processWindow(pg, start, end, verbose).catch((err) => {
        throw { err, start, end };
      })
    )
  );

  const failures = results
    .map((res, idx) => ({ res, idx }))
    .filter(({ res }) => res.status === "rejected");

  if (failures.length) {
    console.error(`${labelPrefix}Batch ${batchIndex}: ${failures.length} window(s) failed`);
    for (const { res, idx } of failures) {
      const failure = res;
      const window = batch[idx];
      const reason = failure.reason?.err ?? failure.reason;
      console.error(
        `   [${idx + 1}] ${window.start.toISO()} -> ${window.end.toISO()} :`,
        reason?.message || reason
      );
    }
    const entries = failures.map(({ res, idx }) => {
      const window = batch[idx];
      const reason = res.reason?.err ?? res.reason;
      return {
        start: window.start.toISO(),
        end: window.end.toISO(),
        message: reason?.message || String(reason),
        raw: reason && reason.stack ? String(reason.stack) : undefined,
        occurredAt: new Date().toISOString()
      };
    });
    recordFailures(entries);
  }
}

async function processDailyRange(pg, rangeStart, rangeEnd, verbose, options = {}) {
  const { batchSize = DAILY_BATCH_SIZE, pauseMs = DAILY_BATCH_DELAY_MS, label = "" } = options;
  if (!rangeStart || !rangeEnd || typeof rangeStart.toMillis !== "function" || typeof rangeEnd.toMillis !== "function") {
    throw new Error("Invalid range boundaries supplied for daily processing");
  }

  const labelPrefix = label ? `[${label}] ` : "";
  const startMs = rangeStart.toMillis();
  const endMs = rangeEnd.toMillis();

  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
    throw new Error("Invalid range boundaries supplied for daily processing");
  }

  if (startMs >= endMs) {
    console.log(`${labelPrefix}No daily windows to process (range start >= range end).`);
    return;
  }

  let cursor = rangeStart;
  let cursorMs = startMs;
  let batchIndex = 0;

  while (cursorMs < endMs) {
    const batch = [];
    while (batch.length < batchSize && cursorMs < endMs) {
      const dayEnd = DateTime.min(cursor.plus({ days: 1 }), rangeEnd);
      batch.push({ start: cursor, end: dayEnd });
      cursor = dayEnd;
      cursorMs = cursor.toMillis();
    }

    batchIndex++;
    await processDailyBatch(pg, batch, batchIndex, verbose, label);
    if (pauseMs > 0 && cursorMs < endMs) await sleep(pauseMs);
  }
}

async function runPreviousMonthDaily(pg, verbose, label = "Monthly") {
  const now = DateTime.now().setZone(TZ);
  const monthEnd = now.startOf("month");
  const monthStart = monthEnd.minus({ months: 1 });
  const labelPrefix = label ? `[${label}] ` : "";
  console.log(`${labelPrefix}Preparing daily windows for ${monthStart.toISO()} -> ${monthEnd.toISO()}...`);
  await processDailyRange(pg, monthStart, monthEnd, verbose, { pauseMs: 0, label });
}

async function backfillThenSchedule(pg, verbose) {
  // 1-day windows, 30 at a time, from 2025-09-06 -> 2350-01-01 (override with --from/--to)
  const bfStart = parseRangeBoundary("backfill start", argv.from, DEFAULT_BACKFILL_START_ISO);
  const bfEnd = parseRangeBoundary("backfill end", argv.to, DEFAULT_BACKFILL_END_ISO);

  await processDailyRange(pg, bfStart, bfEnd, verbose, { label: "Backfill" });

  console.log("Backfill complete. Scheduling monthly polling...");
  // 1st of every month at 00:00 local time
  const job = new CronJob(
    "0 0 1 * *",
    async () => {
      try {
        await runPreviousMonthDaily(pg, verbose);
      } catch (e) {
        console.error("Monthly poll error:", e.message || e);
      }
    },
    null,
    true,
    TZ
  );

  // Immediate pass for the last month
  await runPreviousMonthDaily(pg, verbose, "Monthly (initial)");

  job.start();
  console.log("Running. Press Ctrl+C to exit.");
  process.stdin.resume();
}

/** ===== Main ===== */
(async () => {
  const pg = new Client(PG);
  await pg.connect();
  await ensureTable(pg);

  if (argv.test) {
    // Your exact example: 02/07/2000 12:00 AM -> 02/08/2000 12:00 AM (Pacific)
    const start = DateTime.fromFormat("02/07/2000 12:00 AM", "MM/dd/yyyy hh:mm a", { zone: TZ });
    const end   = DateTime.fromFormat("02/08/2000 12:00 AM", "MM/dd/yyyy hh:mm a", { zone: TZ });
    console.log("Running TEST window (writes to DB) ...");
    await processWindow(pg, start, end, argv.verbose);
    console.log("TEST complete.");
    await pg.end();
    return;
  }

  await backfillThenSchedule(pg, argv.verbose);
})();
