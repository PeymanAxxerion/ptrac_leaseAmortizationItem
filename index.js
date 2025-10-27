import axios from "axios";
import { Client } from "pg";
import { DateTime } from "luxon";
import { CronJob } from "cron";
import express from "express";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import fs from "fs";
import path from "path";

/** ===== Config ===== */
const AX_URL = "https://ptrac.axxerion.us/webservices/ptrac/rest/functions/completereportresult";
const AX_AUTH_BASIC = "Basic YXh1cHRyYWM6UXJlcG9pITU1";
const AX_REFERENCE = "PTR-REP-783";
const PTR_REP_788_REFERENCE = "PTR-REP-788";
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
const DEFAULT_PORT = 3210;
const PTR_REP_788_SWEEP_MINUTES = 15;
const PTR_REP_788_PER_CALL_DELAY_MS = 60_000;

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
  .option("serve", { type: "boolean", default: false, desc: "Start HTTP server with manual endpoints" })
  .option("port", { type: "number", default: DEFAULT_PORT, desc: "Port for --serve mode" })
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

async function callAxxerion(startDt, endDt, options = {}) {
  const {
    reference = AX_REFERENCE,
    fieldStart = argv["field-start"],
    fieldEnd = argv["field-end"],
    logHttp = argv["log-http"]
  } = options;

  const body = {
    reference,
    filterFields: [fieldStart, fieldEnd],
    filterValues: [fmtAxxerion(startDt), fmtAxxerion(endDt)]
  };

  const payload = JSON.stringify(body);

  if (logHttp) {
    console.log(`[${reference}] HTTP POST`, AX_URL);
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

  if (logHttp) {
    console.log(`[${reference}] -> Status:`, resp.status, resp.statusText);
    console.log(`[${reference}] -> Resp first 400 chars:`, String(resp.data).slice(0, 400));
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
async function upsertByKey(pg, records, winStart, winEnd, verbose, options = {}) {
  const destTable = options.destTable ?? DEST_TABLE;
  const sql = `
    INSERT INTO ${destTable}(id, created_at, updated_at, data)
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

async function processWindow(pg, startDt, endDt, verbose, options = {}) {
  const {
    reference = AX_REFERENCE,
    fieldStart = argv["field-start"],
    fieldEnd = argv["field-end"],
    dump = argv.dump,
    logHttp = argv["log-http"],
    destTable
  } = options;
  const labelPrefix = reference ? `[${reference}] ` : "";

  console.log(`${labelPrefix}-> Fetch ${startDt.setZone(TZ).toISO()} .. ${endDt.setZone(TZ).toISO()}`);

  const rawText = await callAxxerion(startDt.toJSDate(), endDt.toJSDate(), {
    reference,
    fieldStart,
    fieldEnd,
    logHttp
  });
  const { rows, raw, parsed } = normalizeRecords(rawText);

  if (dump) {
    const dir = path.join(process.cwd(), "dumps");
    ensureDir(dir);
    const f = path.join(
      dir,
      `${startDt.toISO({ suppressMilliseconds: true })}__${endDt.toISO({ suppressMilliseconds: true })}.json`
        .replace(/[:]/g, "-")
    );
    // Dump the raw text as-is so we can inspect any non-JSON wrappers
    fs.writeFileSync(f, typeof raw === "string" ? raw : String(raw));
    if (verbose) console.log(`${labelPrefix}   wrote raw dump:`, f);
  }

  console.log(`${labelPrefix}   records_from_api=${rows.length}`);
  if (rows.length) {
    console.log(`${labelPrefix}   sample_keys:`, Object.keys(rows[0]).slice(0, 20).join(", "));
    const { loaded, skippedNoKey } = await upsertByKey(pg, rows, startDt, endDt, verbose, { destTable });
    if (skippedNoKey) {
      console.log(`${labelPrefix}   loaded_to_db=${loaded} (skipped_missing_key=${skippedNoKey})`);
    } else {
      console.log(`${labelPrefix}   loaded_to_db=${loaded}`);
    }
  } else {
    // If parsed is null but raw contains "data":[ ... ] we should have caught it;
    // log a hint showing the first 200 chars to help diagnose further edge cases.
    if (!parsed) {
      console.log(`${labelPrefix}   WARN: could not parse payload; first 200 chars:`);
      console.log(`${labelPrefix}${String(raw).slice(0, 200)}`);
    }
    console.log(`${labelPrefix}   (no DB writes; empty window)`);
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

/** ===== PTR-REP-788 Minute Sweep ===== */
const ptrRep788State = {
  job: null,
  startedAtIso: null,
  lastRequestedAtIso: null,
  lastRunStartedAtIso: null,
  lastRunFinishedAtIso: null,
  lastError: null,
  isTickRunning: false,
  preferredVerbose: argv.verbose,
  delayPerCallMs: PTR_REP_788_PER_CALL_DELAY_MS,
  lastCatchupRequestedAtIso: null,
  lastCatchupCompletedAtIso: null,
  lastCatchupDays: null
};

function buildBackwardMinuteWindows(minutes, pivot = DateTime.now().setZone(TZ)) {
  const totalMinutes = Math.max(0, Math.floor(Number.isFinite(minutes) ? minutes : 0));
  const windows = [];
  if (totalMinutes === 0) return windows;
  const anchor = pivot.startOf("minute");
  for (let i = 0; i < totalMinutes; i++) {
    const windowEnd = anchor.minus({ minutes: i });
    const windowStart = windowEnd.minus({ minutes: 1 });
    windows.push({ start: windowStart, end: windowEnd });
  }
  return windows;
}

async function runPtrRep788Sweep(pg, options = {}) {
  const {
    minutes = PTR_REP_788_SWEEP_MINUTES,
    pivot = DateTime.now().setZone(TZ),
    verbose = false,
    delayMs,
    reference = PTR_REP_788_REFERENCE,
    label
  } = options;

  const minuteVerbose = Boolean(verbose);
  const windows = buildBackwardMinuteWindows(minutes, pivot);
  if (!windows.length) {
    console.log(`[${reference}] No minute windows to process.`);
    return;
  }

  const nowIso = DateTime.now().setZone(TZ).toISO();
  const prefix = `[${label ?? reference}]`;
  console.log(`${prefix} Starting backward sweep (${windows.length} minute window(s)) at ${nowIso}`);

  const perCallDelay = (() => {
    if (delayMs != null && Number.isFinite(Number(delayMs))) return Math.max(0, Number(delayMs));
    if (Number.isFinite(ptrRep788State.delayPerCallMs)) return Math.max(0, ptrRep788State.delayPerCallMs);
    return PTR_REP_788_PER_CALL_DELAY_MS;
  })();

  for (let i = 0; i < windows.length; i++) {
    const { start, end } = windows[i];
    try {
      await processWindow(pg, start, end, minuteVerbose, {
        reference,
        dump: false,
        logHttp: false
      });
    } catch (err) {
      const message = err?.message || String(err);
      console.error(`${prefix} Minute window failure ${start.toISO()} -> ${end.toISO()}:`, message);
      recordFailures([
        {
          start: start.toISO(),
          end: end.toISO(),
          message,
          raw: err?.stack ? String(err.stack) : undefined,
          occurredAt: new Date().toISOString()
        }
      ]);
    }
    if (i < windows.length - 1 && perCallDelay > 0) await sleep(perCallDelay);
  }

  console.log(`${prefix} Backward sweep complete at ${DateTime.now().setZone(TZ).toISO()}`);
}

async function safeRunPtrRep788Sweep(pg, verboseOverride, sweepOptions = {}) {
  if (ptrRep788State.isTickRunning) {
    console.log(`[${PTR_REP_788_REFERENCE}] Sweep already in progress; skipping new trigger.`);
    return false;
  }
  ptrRep788State.isTickRunning = true;
  if (!ptrRep788State.startedAtIso) ptrRep788State.startedAtIso = new Date().toISOString();
  ptrRep788State.lastRunStartedAtIso = new Date().toISOString();
  try {
    const effectiveVerbose =
      typeof verboseOverride === "boolean" ? verboseOverride : Boolean(sweepOptions.verbose ?? ptrRep788State.preferredVerbose);
    const mergedOptions = { verbose: effectiveVerbose, ...sweepOptions };
    await runPtrRep788Sweep(pg, mergedOptions);
    ptrRep788State.lastError = null;
    return true;
  } catch (err) {
    ptrRep788State.lastError = err?.message || String(err);
    console.error(`[${PTR_REP_788_REFERENCE}] Sweep error:`, err?.stack || err);
    return false;
  } finally {
    ptrRep788State.isTickRunning = false;
    ptrRep788State.lastRunFinishedAtIso = new Date().toISOString();
  }
}

function ensurePtrRep788Job(pg) {
  if (!ptrRep788State.job) {
    ptrRep788State.job = new CronJob(
      "*/15 * * * *",
      () => {
        void safeRunPtrRep788Sweep(pg, ptrRep788State.preferredVerbose);
      },
      null,
      false,
      TZ
    );
  }
  return ptrRep788State.job;
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

async function startHttpServer(pg, defaultVerbose, port) {
  const app = express();
  app.use(express.json());

  const routeBase = `/api/reports/${PTR_REP_788_REFERENCE}`;

  const coerceBoolean = (value, fallback = false) => {
    if (typeof value === "boolean") return value;
    if (typeof value === "number") return value !== 0;
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      if (["true", "1", "yes", "y"].includes(normalized)) return true;
      if (["false", "0", "no", "n"].includes(normalized)) return false;
    }
    return fallback;
  };

  const buildStatus = () => {
    let nextRunIso = null;
    const job = ptrRep788State.job;
    if (job && typeof job.nextDates === "function") {
      try {
        const next = job.nextDates();
        if (Array.isArray(next)) {
          const first = next[0];
          nextRunIso = first?.toISO?.() ?? first?.toISOString?.() ?? (first ? String(first) : null);
        } else {
          nextRunIso = next?.toISO?.() ?? next?.toISOString?.() ?? (next ? String(next) : null);
        }
      } catch (_) {
        nextRunIso = null;
      }
    }

    return {
      reference: PTR_REP_788_REFERENCE,
      status: ptrRep788State.isTickRunning
        ? "running"
        : ptrRep788State.job?.running
        ? "scheduled"
        : "idle",
      startedAt: ptrRep788State.startedAtIso,
      lastRequestedAt: ptrRep788State.lastRequestedAtIso,
      lastRunStartedAt: ptrRep788State.lastRunStartedAtIso,
      lastRunFinishedAt: ptrRep788State.lastRunFinishedAtIso,
      nextRun: nextRunIso,
      isTickRunning: ptrRep788State.isTickRunning,
      lastError: ptrRep788State.lastError,
      preferredVerbose: ptrRep788State.preferredVerbose,
      minutesPerSweep: PTR_REP_788_SWEEP_MINUTES,
      delayPerCallMs: ptrRep788State.delayPerCallMs,
      lastCatchupRequestedAt: ptrRep788State.lastCatchupRequestedAtIso,
      lastCatchupCompletedAt: ptrRep788State.lastCatchupCompletedAtIso,
      lastCatchupDays: ptrRep788State.lastCatchupDays
    };
  };

  app.post(`${routeBase}/refresh`, (req, res) => {
    const job = ensurePtrRep788Job(pg);
    const nowIso = new Date().toISOString();
    ptrRep788State.lastRequestedAtIso = nowIso;

    if (req?.body && typeof req.body === "object") {
      if (Object.prototype.hasOwnProperty.call(req.body, "verbose")) {
        ptrRep788State.preferredVerbose = coerceBoolean(req.body.verbose, ptrRep788State.preferredVerbose);
      }
    }
    if (Object.prototype.hasOwnProperty.call(req.query, "verbose")) {
      ptrRep788State.preferredVerbose = coerceBoolean(req.query.verbose, ptrRep788State.preferredVerbose);
    }

    const delayCandidate =
      (req.body && Object.prototype.hasOwnProperty.call(req.body, "delayMs")) ? req.body.delayMs : req.query.delayMs;
    if (typeof delayCandidate !== "undefined") {
      const parsedDelay = Number(delayCandidate);
      if (Number.isFinite(parsedDelay) && parsedDelay >= 0) {
        ptrRep788State.delayPerCallMs = parsedDelay;
      }
    }

    const force = coerceBoolean(req.query.force ?? req.body?.force, false);
    const wasRunning = job.running;
    if (!job.running) {
      job.start();
    }

    let triggeredImmediate = false;
    if (!ptrRep788State.isTickRunning) {
      triggeredImmediate = true;
      void safeRunPtrRep788Sweep(pg, ptrRep788State.preferredVerbose);
    } else if (force) {
      console.log(
        `[${PTR_REP_788_REFERENCE}] Force refresh requested but sweep already running; skipping additional trigger.`
      );
    }

    const status = buildStatus();
    status.forceRequested = force;
    status.triggeredImmediate = triggeredImmediate;
    status.jobWasRunning = wasRunning;
    res.status(wasRunning ? 202 : 201).json(status);
  });

  app.post(`${routeBase}/catchup`, (req, res) => {
    const job = ensurePtrRep788Job(pg);
    const nowIso = new Date().toISOString();
    ptrRep788State.lastRequestedAtIso = nowIso;

    const rawDays = req.body?.days ?? req.query?.days;
    const days = Number(rawDays);
    if (!Number.isFinite(days) || days <= 0) {
      return res.status(400).json({
        error: "Parameter 'days' must be a positive number.",
        received: rawDays
      });
    }

    ptrRep788State.lastCatchupRequestedAtIso = nowIso;
    ptrRep788State.lastCatchupDays = days;

    if (Object.prototype.hasOwnProperty.call(req.body ?? {}, "verbose")) {
      ptrRep788State.preferredVerbose = coerceBoolean(
        req.body.verbose,
        ptrRep788State.preferredVerbose
      );
    }
    if (Object.prototype.hasOwnProperty.call(req.query ?? {}, "verbose")) {
      ptrRep788State.preferredVerbose = coerceBoolean(
        req.query.verbose,
        ptrRep788State.preferredVerbose
      );
    }

    const delayCandidate =
      (req.body && Object.prototype.hasOwnProperty.call(req.body, "delayMs")) ? req.body.delayMs : req.query?.delayMs;
    let delayMsOverride;
    if (typeof delayCandidate !== "undefined") {
      const parsedDelay = Number(delayCandidate);
      if (Number.isFinite(parsedDelay) && parsedDelay >= 0) {
        ptrRep788State.delayPerCallMs = parsedDelay;
        delayMsOverride = parsedDelay;
      }
    }

    const verbose = ptrRep788State.preferredVerbose;
    const totalMinutes = Math.ceil(days * 24 * 60);
    if (!Number.isFinite(totalMinutes) || totalMinutes <= 0) {
      return res.status(400).json({
        error: "Calculated minutes to process is invalid.",
        days,
        totalMinutes
      });
    }

    const jobWasRunning = job.running;
    if (!job.running) job.start();

    const alreadyRunning = ptrRep788State.isTickRunning;
    const pivot = DateTime.now().setZone(TZ);
    const label = `${PTR_REP_788_REFERENCE} catchup(${days}d)`;
    const sweepOptions = { minutes: totalMinutes, pivot, label };
    if (delayMsOverride != null) sweepOptions.delayMs = delayMsOverride;

    safeRunPtrRep788Sweep(pg, verbose, sweepOptions)
      .then((triggered) => {
        if (triggered) ptrRep788State.lastCatchupCompletedAtIso = new Date().toISOString();
      })
      .catch((err) => {
        ptrRep788State.lastError = err?.message || String(err);
        console.error(`[${PTR_REP_788_REFERENCE}] Catchup sweep error:`, err?.stack || err);
      });

    const status = buildStatus();
    status.catchupRequestedDays = days;
    status.minutesToProcess = totalMinutes;
    status.catchupTriggered = !alreadyRunning;
    status.jobWasRunning = jobWasRunning;
    res.status(202).json(status);
  });

  app.get(`${routeBase}/status`, (_req, res) => {
    res.json(buildStatus());
  });

  ptrRep788State.preferredVerbose = coerceBoolean(defaultVerbose, true);

  await new Promise((resolve) => {
    app.listen(port, () => {
      console.log(`HTTP server listening on port ${port}`);
      console.log(`POST ${routeBase}/refresh to start the backward sweep scheduler.`);
      console.log(`GET  ${routeBase}/status  to inspect scheduler state.`);
      resolve();
    });
  });
}

/** ===== Main ===== */
(async () => {
  const pg = new Client(PG);
  await pg.connect();
  await ensureTable(pg);

  if (argv.serve) {
    await startHttpServer(pg, argv.verbose, argv.port);
    return;
  }

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
