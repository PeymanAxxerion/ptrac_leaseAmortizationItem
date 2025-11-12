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
const TZ = "America/Los_Angeles";

const DEFAULT_CLIENT_REFERENCE = "ptrac";
const DEFAULT_CLIENT_KEY = "ptrac";
const DEFAULT_AX_USERNAME = "axuptrac";
const DEFAULT_AX_PASSWORD = "Qrepoi!55";
const DEFAULT_BATCH_REPORT_REF = "PTR-REP-783";
const DEFAULT_REFRESH_REPORT_REF = "PTR-REP-788";
const DEFAULT_LEASE_REPORT_REF = "PTR-REP-799";
const DEFAULT_REGION_REPORT_REF = "PTR-REP-807";
const DEFAULT_BACKFILL_START_ISO = "2020-01-01T00:00:00";
const DEFAULT_BACKFILL_END_ISO = "2550-01-01T00:00:00";
const DEFAULT_PORT = 3210;
const PTR_REP_788_SWEEP_MINUTES = 15;
const PTR_REP_788_PER_CALL_DELAY_MS = 60_000;

const configState = {
  clientReference: DEFAULT_CLIENT_REFERENCE,
  clientKey: DEFAULT_CLIENT_KEY,
  username: DEFAULT_AX_USERNAME,
  password: DEFAULT_AX_PASSWORD,
  batchReportRef: DEFAULT_BATCH_REPORT_REF,
  refreshReportRef: DEFAULT_REFRESH_REPORT_REF,
  leaseReportRef: DEFAULT_LEASE_REPORT_REF,
  regionReportRef: DEFAULT_REGION_REPORT_REF,
  batchStartIso: DEFAULT_BACKFILL_START_ISO,
  batchEndIso: DEFAULT_BACKFILL_END_ISO
};

const getClientReference = () => configState.clientReference?.trim() || DEFAULT_CLIENT_REFERENCE;
const getClientKey = () => configState.clientKey?.trim() || DEFAULT_CLIENT_KEY;
const getBatchReportRef = () => configState.batchReportRef?.trim() || DEFAULT_BATCH_REPORT_REF;
const getRefreshReportRef = () => configState.refreshReportRef?.trim() || DEFAULT_REFRESH_REPORT_REF;
const getLeaseReportRef = () => configState.leaseReportRef?.trim() || DEFAULT_LEASE_REPORT_REF;
const getRegionReportRef = () => configState.regionReportRef?.trim() || DEFAULT_REGION_REPORT_REF;
const getBatchStartIso = () => configState.batchStartIso?.trim() || DEFAULT_BACKFILL_START_ISO;
const getBatchEndIso = () => configState.batchEndIso?.trim() || DEFAULT_BACKFILL_END_ISO;

const buildAxUrl = () =>
  `https://${getClientReference()}.axxerion.us/webservices/${getClientKey()}/rest/functions/completereportresult`;
const buildAuthHeader = () =>
  `Basic ${Buffer.from(`${configState.username ?? ""}:${configState.password ?? ""}`).toString("base64")}`;

const PG = {
  host: "195.35.11.138",
  port: 5440,
  user: "postgres",
  password: "AXU001",
  database: "axdb",
  ssl: false
};

const DEST_TABLE = "public.ptrac_leaseAmortizationItem";
const LEASE_ITEMS_TABLE = "public.etLeaseAmortizationItem";
const REGION_TABLE = "public.ptregion";
const LEASE_CONTRACTS_REPORT_REF = "PTR-REP-801";
const MAX_LEASE_ERRORS_TRACKED = 50;
const FAILURE_DIR = path.join(process.cwd(), "results");
const FAILURE_JSON_PATH = path.join(FAILURE_DIR, "failed-windows.json");
const FAILURE_HTML_PATH = path.join(FAILURE_DIR, "failed-windows.html");

const failureRecords = [];
const DAILY_BATCH_SIZE = 90;
const DAILY_BATCH_DELAY_MS = 1500;

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

const createAbortError = (message = "Operation cancelled") => {
  const err = new Error(message);
  err.name = "AbortError";
  err.code = "ERR_OPERATION_CANCELLED";
  return err;
};

const isAbortError = (err) => {
  if (!err) return false;
  if (err.name === "AbortError") return true;
  if (err.code === "ERR_OPERATION_CANCELLED") return true;
  if (err.code === "ERR_CANCELED") return true; // axios
  if (typeof err.message === "string" && err.message.toLowerCase().includes("aborted")) return true;
  if (err.constructor && err.constructor.name === "CanceledError") return true;
  return false;
};

function updateConfig(partial = {}) {
  if (partial.clientReference !== undefined) configState.clientReference = String(partial.clientReference ?? "").trim();
  if (partial.clientKey !== undefined) configState.clientKey = String(partial.clientKey ?? "").trim();
  if (partial.username !== undefined) configState.username = String(partial.username ?? "").trim();
  if (partial.password !== undefined) configState.password = String(partial.password ?? "");
  if (partial.batchReportRef !== undefined) configState.batchReportRef = String(partial.batchReportRef ?? "").trim();
  if (partial.refreshReportRef !== undefined) configState.refreshReportRef = String(partial.refreshReportRef ?? "").trim();
  if (partial.leaseReportRef !== undefined) configState.leaseReportRef = String(partial.leaseReportRef ?? "").trim();
  if (partial.regionReportRef !== undefined) configState.regionReportRef = String(partial.regionReportRef ?? "").trim();
  if (partial.batchStartIso !== undefined) configState.batchStartIso = String(partial.batchStartIso ?? "").trim();
  if (partial.batchEndIso !== undefined) configState.batchEndIso = String(partial.batchEndIso ?? "").trim();
}

const getConfigSnapshot = () => ({
  clientReference: getClientReference(),
  clientKey: getClientKey(),
  username: configState.username ?? "",
  password: configState.password ?? "",
  batchReportRef: getBatchReportRef(),
  refreshReportRef: getRefreshReportRef(),
  leaseReportRef: getLeaseReportRef(),
  regionReportRef: getRegionReportRef(),
  batchStartIso: getBatchStartIso(),
  batchEndIso: getBatchEndIso()
});

const activityLog = [];
const MAX_ACTIVITY_ENTRIES = 100;

function createActivityEntry({ functionName, reportReference, status = "running" }) {
  const entry = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
    clientReference: getClientReference(),
    clientKey: getClientKey(),
    functionName,
    reportReference,
    status,
    startedAt: new Date().toISOString(),
    completedAt: null,
    error: null
  };
  activityLog.unshift(entry);
  if (activityLog.length > MAX_ACTIVITY_ENTRIES) activityLog.pop();
  return entry;
}

function updateActivityEntry(id, updates = {}) {
  const entry = activityLog.find((e) => e.id === id);
  if (!entry) return null;
  Object.assign(entry, updates);
  return entry;
}

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

function extractLeaseReference(rec) {
  if (!rec || typeof rec !== "object") return null;
  const candidates = [
    rec.leaseReference,
    rec.LeaseReference,
    rec.reference,
    rec.Reference,
    rec.contractReference,
    rec.contractreference,
    rec.CONTRACTREFERENCE,
    rec.userAttributeReference,
    rec.userattribute_reference,
    rec.user_attribute_reference
  ];
  for (const candidate of candidates) {
    if (candidate != null && String(candidate).trim() !== "") {
      return String(candidate).trim();
    }
  }
  return null;
}


function buildLeaseItemKey(rec, index = 0) {
  const leaseRef =
    rec?.["Contract reference"] ??
    rec?.contractReference ??
    rec?.contract_reference ??
    rec?.leaseReference ??
    rec?.lease_reference ??
    null;
  const amortRef = rec?.["Lease amortization"] ?? rec?.leaseAmortization ?? rec?.lease_amortization ?? null;
  const asAt =
    rec?.["AS AT DATE"] ??
    rec?.asAtDate ??
    rec?.as_at_date ??
    rec?.["Accounting End Date"] ??
    rec?.accountingEndDate ??
    null;
  const year = rec?.Year ?? rec?.year ?? null;
  const month = rec?.Month ?? rec?.month ?? null;
  const baseId = rec?.id ?? rec?.objectid ?? rec?.ObjectId ?? rec?.objectNameId ?? null;

  const parts = [];
  const pushPart = (label, value) => {
    if (value == null) return;
    const norm = String(value).trim();
    if (norm === "") return;
    parts.push(`${label}:${norm}`);
  };

  pushPart("lease", leaseRef);
  pushPart("asat", asAt);
  if (!asAt && (year || month)) {
    const period = `${year ?? "yyyy"}-${month ?? "mm"}`;
    pushPart("period", period);
  }
  pushPart("amort", amortRef);
  pushPart("id", baseId);
  if (!parts.length) parts.push(`row:${index}`);
  return parts.join("::");
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

async function ensureTable(pg, tableName = DEST_TABLE) {
  if (!tableName) throw new Error("Destination table name is required");
  await pg.query(`
    CREATE TABLE IF NOT EXISTS ${tableName}(
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

async function postAxxerion(payload, options = {}) {
  const fallbackReference = typeof payload?.reference === "string" ? payload.reference : undefined;
  const {
    reference = fallbackReference,
    logHttp = argv["log-http"],
    signal,
    contentType = "text/plain"
  } = options;

  if (signal?.aborted) throw createAbortError("Cancelled before issuing Axxerion request");
  const axUrl = buildAxUrl();
  const bodyString = typeof payload === "string" ? payload : JSON.stringify(payload ?? {});
  const labelPrefix = reference ? `[${reference}]` : "[Axxerion]";

  if (logHttp) {
    console.log(`${labelPrefix} HTTP POST`, axUrl);
    console.log("Headers:", { Authorization: "(redacted)", "Content-Type": contentType });
    console.log("Body:", bodyString);
  }

  const resp = await axios.post(axUrl, bodyString, {
    headers: { Authorization: buildAuthHeader(), "Content-Type": contentType },
    responseType: "text",
    transformResponse: [(d) => d],
    timeout: 1800000,
    validateStatus: () => true,
    signal
  });

  if (logHttp) {
    console.log(`${labelPrefix} -> Status:`, resp.status, resp.statusText);
    console.log(`${labelPrefix} -> Resp first 400 chars:`, String(resp.data).slice(0, 400));
  }

  return resp.data;
}

async function callAxxerion(startDt, endDt, options = {}) {
  const {
    reference = getBatchReportRef(),
    fieldStart = argv["field-start"],
    fieldEnd = argv["field-end"],
    logHttp = argv["log-http"],
    signal
  } = options;

  const body = {
    reference,
    filterFields: [fieldStart, fieldEnd],
    filterValues: [fmtAxxerion(startDt), fmtAxxerion(endDt)]
  };

  return postAxxerion(body, {
    reference,
    logHttp,
    signal,
    contentType: "text/plain"
  });
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

async function fetchLeaseContracts(signal) {
  const payload = {
    reference: LEASE_CONTRACTS_REPORT_REF,
    filterFields: ["leaseamortization"],
    filterValues: [{ o: 130, v: "" }]
  };
  const raw = await postAxxerion(payload, {
    reference: LEASE_CONTRACTS_REPORT_REF,
    contentType: "application/json",
    signal
  });
  const normalized = normalizeRecords(raw);
  if (!normalized.parsed && normalized.raw) {
    throw new Error("Unable to parse lease contract list from PTR-REP-801.");
  }
  return normalized.rows ?? [];
}

async function fetchLeaseItemsByReference(leaseReference, signal) {
  if (!leaseReference) return [];
  const payload = {
    reference: getLeaseReportRef(),
    filterFields: ["leaseReference"],
    filterValues: [leaseReference]
  };
  const raw = await postAxxerion(payload, {
    reference: `${getLeaseReportRef()}:${leaseReference}`,
    contentType: "application/json",
    signal
  });
  const normalized = normalizeRecords(raw);
  if (!normalized.parsed && normalized.raw) {
    throw new Error(`Unable to parse amortization items for reference ${leaseReference}.`);
  }
  return normalized.rows ?? [];
}

async function harvestLeaseAmortizationItems(pg, verbose = argv.verbose, options = {}) {
  const signal = options.signal;
  const shouldCancel = options.shouldCancel;
  if (signal?.aborted) throw createAbortError("Lease amortization sync cancelled before start");

  console.log("[LeaseSync] Fetching lease contracts from PTR-REP-801...");
  const contracts = await fetchLeaseContracts(signal);
  console.log(`[LeaseSync] Retrieved ${contracts.length} contract record(s).`);

  const summary = {
    contractsFetched: contracts.length,
    contractsProcessed: 0,
    contractsMissingReference: 0,
    itemsFetched: 0,
    itemsInserted: 0,
    itemsSkipped: 0,
    errorCount: 0,
    errors: [],
    lastReference: null
  };
  leaseLoadState.currentSummary = { ...summary };

  if (!contracts.length) {
    return summary;
  }

  const fallbackStart = DateTime.now().setZone(TZ);
  const fallbackEnd = fallbackStart.plus({ minutes: 1 });

  for (let i = 0; i < contracts.length; i++) {
    if (signal?.aborted || shouldCancel?.()) throw createAbortError("Lease amortization sync cancelled");
    const leaseReference = extractLeaseReference(contracts[i]);
    summary.lastReference = leaseReference;

    if (!leaseReference) {
      summary.contractsMissingReference++;
      summary.errorCount++;
      if (summary.errors.length < MAX_LEASE_ERRORS_TRACKED) {
        summary.errors.push({ reference: null, message: "Missing lease reference on contract record." });
      }
      leaseLoadState.currentSummary = { ...summary };
      continue;
    }

    summary.contractsProcessed++;
    if (verbose) {
      console.log(`[LeaseSync] (${i + 1}/${contracts.length}) Loading amortization items for ${leaseReference}...`);
    }

    let items = [];
    try {
      items = await fetchLeaseItemsByReference(leaseReference, signal);
    } catch (err) {
      const msg = err?.message || String(err);
      summary.errorCount++;
      if (summary.errors.length < MAX_LEASE_ERRORS_TRACKED) {
        summary.errors.push({ reference: leaseReference, message: msg });
      }
      console.error(`[LeaseSync] Failed to fetch amortization items for ${leaseReference}:`, msg);
      leaseLoadState.currentSummary = { ...summary };
      continue;
    }

    summary.itemsFetched += items.length;
    if (!items.length) {
      leaseLoadState.currentSummary = { ...summary };
      continue;
    }

    try {
      const { loaded, skippedNoKey } = await upsertByKey(pg, items, fallbackStart, fallbackEnd, verbose, {
        destTable: LEASE_ITEMS_TABLE,
        signal,
        keySelector: buildLeaseItemKey
      });
      summary.itemsInserted += loaded;
      summary.itemsSkipped += skippedNoKey;
    } catch (err) {
      const msg = err?.message || String(err);
      summary.errorCount++;
      if (summary.errors.length < MAX_LEASE_ERRORS_TRACKED) {
        summary.errors.push({ reference: leaseReference, message: `DB upsert failed: ${msg}` });
      }
      console.error(`[LeaseSync] Failed to persist amortization items for ${leaseReference}:`, msg);
    }

    leaseLoadState.currentSummary = { ...summary };
  }

  return summary;
}

/** Insert/upsert 1-by-1, keyed by id/objectid; skip if no key. No writes on empty window. */
async function upsertByKey(pg, records, winStart, winEnd, verbose, options = {}) {
  const destTable = options.destTable ?? DEST_TABLE;
  const signal = options.signal;
  const shouldCancel = options.shouldCancel;
  const keySelector = typeof options.keySelector === "function" ? options.keySelector : extractKey;
  const sql = `
    INSERT INTO ${destTable}(id, created_at, updated_at, data)
    VALUES ($1,$2,$3,$4)
    ON CONFLICT (id) DO UPDATE
      SET updated_at = EXCLUDED.updated_at,
          data = EXCLUDED.data`;

  let loaded = 0;
  let skippedNoKey = 0;
  for (let i = 0; i < records.length; i++) {
    if (signal?.aborted || shouldCancel?.()) throw createAbortError("Upsert cancelled");
    const r = records[i] ?? {};
    const key = keySelector(r, i);
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
    reference = getBatchReportRef(),
    fieldStart = argv["field-start"],
    fieldEnd = argv["field-end"],
    dump = argv.dump,
    logHttp = argv["log-http"],
    destTable,
    signal,
    shouldCancel
  } = options;
  const labelPrefix = reference ? `[${reference}] ` : "";

  if (signal?.aborted || shouldCancel?.()) throw createAbortError("Window cancelled before start");

  console.log(`${labelPrefix}-> Fetch ${startDt.setZone(TZ).toISO()} .. ${endDt.setZone(TZ).toISO()}`);

  const rawText = await callAxxerion(startDt.toJSDate(), endDt.toJSDate(), {
    reference,
    fieldStart,
    fieldEnd,
    logHttp,
    signal
  });
  const { rows, raw, parsed } = normalizeRecords(rawText);

  if (signal?.aborted || shouldCancel?.()) throw createAbortError("Window cancelled");

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

  const recordsFromApi = rows.length;
  let loaded = 0;
  let skippedNoKey = 0;

  console.log(`${labelPrefix}   records_from_api=${recordsFromApi}`);
  if (recordsFromApi) {
    console.log(`${labelPrefix}   sample_keys:`, Object.keys(rows[0]).slice(0, 20).join(", "));
    const result = await upsertByKey(pg, rows, startDt, endDt, verbose, {
      destTable,
      signal,
      shouldCancel
    });
    loaded = result.loaded;
    skippedNoKey = result.skippedNoKey;
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

  return {
    recordsFromApi,
    loadedToDb: loaded,
    skippedNoKey
  };
}

async function processDailyBatch(pg, batch, batchIndex, verbose, label = "", options = {}) {
  const labelPrefix = label ? `[${label}] ` : "";
  const signal = options.signal;
  const shouldCancel = options.shouldCancel;
  console.log(`${labelPrefix}Processing batch ${batchIndex} (${batch.length} day window(s))...`);

  const failures = [];
  let recordsFromApi = 0;
  let loadedToDb = 0;
  let skippedNoKey = 0;

  for (let i = 0; i < batch.length; i++) {
    if (signal?.aborted || shouldCancel?.()) throw createAbortError("Batch cancelled");
    const window = batch[i];
    try {
      const stats = await processWindow(pg, window.start, window.end, verbose, {
        ...options,
        signal,
        shouldCancel
      });
      recordsFromApi += stats?.recordsFromApi ?? 0;
      loadedToDb += stats?.loadedToDb ?? 0;
      skippedNoKey += stats?.skippedNoKey ?? 0;
    } catch (err) {
      if (isAbortError(err)) throw err;
      const message = err?.message || String(err);
      console.error(
        `${labelPrefix}   [${i + 1}] ${window.start.toISO()} -> ${window.end.toISO()} :`,
        message
      );
      failures.push({
        start: window.start.toISO(),
        end: window.end.toISO(),
        message,
        raw: err?.stack ? String(err.stack) : undefined,
        occurredAt: new Date().toISOString()
      });
    }
  }

  if (failures.length) {
    console.error(`${labelPrefix}Batch ${batchIndex}: ${failures.length} window(s) failed`);
    recordFailures(failures);
  }

  return {
    recordsFromApi,
    loadedToDb,
    skippedNoKey,
    failures
  };
}

async function processDailyRange(pg, rangeStart, rangeEnd, verbose, options = {}) {
  const {
    batchSize = DAILY_BATCH_SIZE,
    pauseMs = DAILY_BATCH_DELAY_MS,
    label = "",
    reference,
    destTable,
    fieldStart,
    fieldEnd,
    dump,
    logHttp,
    signal,
    shouldCancel,
    onProgress
  } = options;
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
    return { recordsFromApi: 0, loadedToDb: 0, skippedNoKey: 0 };
  }

  let cursor = rangeStart;
  let cursorMs = startMs;
  let batchIndex = 0;
  let totalRecordsFromApi = 0;
  let totalLoadedToDb = 0;
  let totalSkippedNoKey = 0;

  while (cursorMs < endMs) {
    if (signal?.aborted || shouldCancel?.()) throw createAbortError("Daily range cancelled");
    const batch = [];
    while (batch.length < batchSize && cursorMs < endMs) {
      const dayEnd = DateTime.min(cursor.plus({ days: 1 }), rangeEnd);
      batch.push({ start: cursor, end: dayEnd });
      cursor = dayEnd;
      cursorMs = cursor.toMillis();
    }

    batchIndex++;
    const batchStats = await processDailyBatch(pg, batch, batchIndex, verbose, label, {
      reference,
      destTable,
      fieldStart,
      fieldEnd,
      dump,
      logHttp,
      signal,
      shouldCancel
    });
    totalRecordsFromApi += batchStats?.recordsFromApi ?? 0;
    totalLoadedToDb += batchStats?.loadedToDb ?? 0;
    totalSkippedNoKey += batchStats?.skippedNoKey ?? 0;
    if (typeof onProgress === "function") {
      onProgress({
        totalRecordsFromApi,
        totalLoadedToDb,
        totalSkippedNoKey,
        deltaRecordsFromApi: batchStats?.recordsFromApi ?? 0,
        deltaLoadedToDb: batchStats?.loadedToDb ?? 0,
        deltaSkippedNoKey: batchStats?.skippedNoKey ?? 0
      });
    }
    if (pauseMs > 0 && cursorMs < endMs) {
      if (signal?.aborted || shouldCancel?.()) throw createAbortError("Daily range cancelled");
      await sleep(pauseMs);
    }
  }

  return {
    recordsFromApi: totalRecordsFromApi,
    loadedToDb: totalLoadedToDb,
    skippedNoKey: totalSkippedNoKey
  };
}

async function runPreviousMonthDaily(pg, verbose, label = "Monthly") {
  const now = DateTime.now().setZone(TZ);
  const monthEnd = now.startOf("month");
  const monthStart = monthEnd.minus({ months: 1 });
  const labelPrefix = label ? `[${label}] ` : "";
  console.log(`${labelPrefix}Preparing daily windows for ${monthStart.toISO()} -> ${monthEnd.toISO()}...`);
  await processDailyRange(pg, monthStart, monthEnd, verbose, {
    pauseMs: 0,
    label,
    reference: getBatchReportRef()
  });
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
  lastCatchupDays: null,
  cancelRequested: false,
  abortController: null,
  lastCancelRequestedAtIso: null,
  lastCancelledAtIso: null,
  currentActivityId: null,
  currentRecordsFromApi: 0,
  currentLoadedToDb: 0,
  currentSkippedNoKey: 0,
  lastRecordsFromApi: 0,
  lastLoadedToDb: 0,
  lastSkippedNoKey: 0
};

const batchLoadState = {
  isRunning: false,
  lastRequestedAtIso: null,
  lastRunStartedAtIso: null,
  lastRunFinishedAtIso: null,
  lastError: null,
  lastStartIso: null,
  lastEndIso: null,
  cancelRequested: false,
  abortController: null,
  lastCancelRequestedAtIso: null,
  lastCancelledAtIso: null,
  currentActivityId: null,
  currentRecordsFromApi: 0,
  currentLoadedToDb: 0,
  currentSkippedNoKey: 0,
  lastRecordsFromApi: 0,
  lastLoadedToDb: 0,
  lastSkippedNoKey: 0
};

const leaseLoadState = {
  isRunning: false,
  cancelRequested: false,
  abortController: null,
  lastRequestedAtIso: null,
  lastRunStartedAtIso: null,
  lastRunFinishedAtIso: null,
  lastError: null,
  lastCancelRequestedAtIso: null,
  lastCancelledAtIso: null,
  currentActivityId: null,
  currentSummary: null,
  lastSummary: null
};

const regionLoadState = {
  isRunning: false,
  cancelRequested: false,
  abortController: null,
  lastRequestedAtIso: null,
  lastRunStartedAtIso: null,
  lastRunFinishedAtIso: null,
  lastError: null,
  lastStartIso: null,
  lastEndIso: null,
  lastCancelRequestedAtIso: null,
  lastCancelledAtIso: null,
  currentActivityId: null,
  currentSummary: null,
  lastSummary: null,
  currentRecordsFromApi: 0,
  currentLoadedToDb: 0,
  currentSkippedNoKey: 0,
  lastRecordsFromApi: 0,
  lastLoadedToDb: 0,
  lastSkippedNoKey: 0
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
    reference = getRefreshReportRef(),
    label,
    signal,
    shouldCancel
  } = options;

  const minuteVerbose = Boolean(verbose);
  const windows = buildBackwardMinuteWindows(minutes, pivot);
  if (!windows.length) {
    console.log(`[${reference}] No minute windows to process.`);
    ptrRep788State.currentRecordsFromApi = 0;
    ptrRep788State.currentLoadedToDb = 0;
    ptrRep788State.currentSkippedNoKey = 0;
    return { cancelled: false, recordsFromApi: 0, loadedToDb: 0, skippedNoKey: 0 };
  }

  const nowIso = DateTime.now().setZone(TZ).toISO();
  const prefix = `[${label ?? reference}]`;
  console.log(`${prefix} Starting backward sweep (${windows.length} minute window(s)) at ${nowIso}`);

  const perCallDelay = (() => {
    if (delayMs != null && Number.isFinite(Number(delayMs))) return Math.max(0, Number(delayMs));
    if (Number.isFinite(ptrRep788State.delayPerCallMs)) return Math.max(0, ptrRep788State.delayPerCallMs);
    return PTR_REP_788_PER_CALL_DELAY_MS;
  })();

  let totalRecordsFromApi = 0;
  let totalLoadedToDb = 0;
  let totalSkippedNoKey = 0;
  ptrRep788State.currentRecordsFromApi = 0;
  ptrRep788State.currentLoadedToDb = 0;
  ptrRep788State.currentSkippedNoKey = 0;

  for (let i = 0; i < windows.length; i++) {
    if (signal?.aborted || shouldCancel?.()) {
      console.log(`${prefix} Cancellation requested; stopping sweep.`);
      return {
        cancelled: true,
        recordsFromApi: totalRecordsFromApi,
        loadedToDb: totalLoadedToDb,
        skippedNoKey: totalSkippedNoKey
      };
    }
    const { start, end } = windows[i];
    try {
      const stats = await processWindow(pg, start, end, minuteVerbose, {
        reference,
        dump: false,
        logHttp: false,
        signal,
        shouldCancel
      });
      totalRecordsFromApi += stats?.recordsFromApi ?? 0;
      totalLoadedToDb += stats?.loadedToDb ?? 0;
      totalSkippedNoKey += stats?.skippedNoKey ?? 0;
      ptrRep788State.currentRecordsFromApi = totalRecordsFromApi;
      ptrRep788State.currentLoadedToDb = totalLoadedToDb;
      ptrRep788State.currentSkippedNoKey = totalSkippedNoKey;
    } catch (err) {
      if (isAbortError(err)) {
        console.log(`${prefix} Sweep cancelled during window ${start.toISO()} -> ${end.toISO()}.`);
        return {
          cancelled: true,
          recordsFromApi: totalRecordsFromApi,
          loadedToDb: totalLoadedToDb,
          skippedNoKey: totalSkippedNoKey
        };
      }
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
  return {
    cancelled: false,
    recordsFromApi: totalRecordsFromApi,
    loadedToDb: totalLoadedToDb,
    skippedNoKey: totalSkippedNoKey
  };
}

async function safeRunPtrRep788Sweep(pg, verboseOverride, sweepOptions = {}) {
  const reference = typeof sweepOptions.reference === "string" && sweepOptions.reference.trim() !== ""
    ? sweepOptions.reference.trim()
    : getRefreshReportRef();
  if (ptrRep788State.isTickRunning) {
    console.log(`[${reference}] Sweep already in progress; skipping new trigger.`);
    return "skipped";
  }
  ptrRep788State.isTickRunning = true;
  ptrRep788State.cancelRequested = false;
  const controller = new AbortController();
  ptrRep788State.abortController = controller;
  if (!ptrRep788State.startedAtIso) ptrRep788State.startedAtIso = new Date().toISOString();
  ptrRep788State.lastRunStartedAtIso = new Date().toISOString();
  ptrRep788State.currentRecordsFromApi = 0;
  ptrRep788State.currentLoadedToDb = 0;
  ptrRep788State.currentSkippedNoKey = 0;
  try {
    const effectiveVerbose =
      typeof verboseOverride === "boolean" ? verboseOverride : Boolean(sweepOptions.verbose ?? ptrRep788State.preferredVerbose);
    const mergedOptions = {
      ...sweepOptions,
      reference,
      verbose: effectiveVerbose,
      signal: controller.signal,
      shouldCancel: () => ptrRep788State.cancelRequested
    };
    const result = await runPtrRep788Sweep(pg, mergedOptions);
    ptrRep788State.lastError = null;
    if (result) {
      if (typeof result.recordsFromApi === "number") {
        ptrRep788State.currentRecordsFromApi = result.recordsFromApi;
      }
      if (typeof result.loadedToDb === "number") {
        ptrRep788State.currentLoadedToDb = result.loadedToDb;
      }
      if (typeof result.skippedNoKey === "number") {
        ptrRep788State.currentSkippedNoKey = result.skippedNoKey;
      }
      ptrRep788State.lastRecordsFromApi = ptrRep788State.currentRecordsFromApi;
      ptrRep788State.lastLoadedToDb = ptrRep788State.currentLoadedToDb;
      ptrRep788State.lastSkippedNoKey = ptrRep788State.currentSkippedNoKey;
    }
    if (result?.cancelled) {
      ptrRep788State.lastCancelledAtIso = new Date().toISOString();
      return "cancelled";
    }
    return "completed";
  } catch (err) {
    if (isAbortError(err)) {
      ptrRep788State.lastError = null;
      ptrRep788State.lastCancelledAtIso = new Date().toISOString();
      ptrRep788State.lastRecordsFromApi = ptrRep788State.currentRecordsFromApi;
      ptrRep788State.lastLoadedToDb = ptrRep788State.currentLoadedToDb;
      ptrRep788State.lastSkippedNoKey = ptrRep788State.currentSkippedNoKey;
      return "cancelled";
    }
    ptrRep788State.lastError = err?.message || String(err);
    console.error(`[${reference}] Sweep error:`, err?.stack || err);
    ptrRep788State.lastRecordsFromApi = ptrRep788State.currentRecordsFromApi;
    ptrRep788State.lastLoadedToDb = ptrRep788State.currentLoadedToDb;
    ptrRep788State.lastSkippedNoKey = ptrRep788State.currentSkippedNoKey;
    throw err;
  } finally {
    ptrRep788State.cancelRequested = false;
    ptrRep788State.abortController = null;
    ptrRep788State.isTickRunning = false;
    ptrRep788State.lastRunFinishedAtIso = new Date().toISOString();
  }
}

function ensurePtrRep788Job(pg) {
  if (!ptrRep788State.job) {
    ptrRep788State.job = new CronJob(
      "*/15 * * * *",
      () => {
        void safeRunPtrRep788Sweep(pg, ptrRep788State.preferredVerbose, {
          reference: getRefreshReportRef()
        });
      },
      null,
      false,
      TZ
    );
  }
  return ptrRep788State.job;
}

function requestPtrRep788Cancellation() {
  if (!ptrRep788State.isTickRunning) {
    return { accepted: false, reason: "not_running" };
  }
  if (ptrRep788State.cancelRequested) {
    return { accepted: true, reason: "already_requested" };
  }
  ptrRep788State.cancelRequested = true;
  ptrRep788State.lastCancelRequestedAtIso = new Date().toISOString();
  const controller = ptrRep788State.abortController;
  if (controller) {
    controller.abort();
  }
  return { accepted: true, reason: "requested" };
}

async function safeRunBatchLoad(pg, options = {}) {
  const reference = typeof options.reference === "string" && options.reference.trim() !== ""
    ? options.reference.trim()
    : getBatchReportRef();
  if (batchLoadState.isRunning) {
    console.log(`[${reference}] Batch load already running; skipping new trigger.`);
    return "skipped";
  }

  batchLoadState.isRunning = true;
  batchLoadState.cancelRequested = false;
  const controller = new AbortController();
  batchLoadState.abortController = controller;
  batchLoadState.lastRunStartedAtIso = new Date().toISOString();
  batchLoadState.lastError = null;

  const startIso = options.start ?? getBatchStartIso();
  const endIso = options.end ?? getBatchEndIso();
  batchLoadState.lastStartIso = startIso;
  batchLoadState.lastEndIso = endIso;
  batchLoadState.currentRecordsFromApi = 0;
  batchLoadState.currentLoadedToDb = 0;
  batchLoadState.currentSkippedNoKey = 0;

  try {
    const rangeStart = parseRangeBoundary("batch start", startIso, getBatchStartIso());
    const rangeEnd = parseRangeBoundary("batch end", endIso, getBatchEndIso());
    const verbose = typeof options.verbose === "boolean"
      ? options.verbose
      : Boolean(options.verbose ?? argv.verbose);

    const totals = await processDailyRange(pg, rangeStart, rangeEnd, verbose, {
      label: options.label ?? "BatchLoad",
      reference,
      signal: controller.signal,
      shouldCancel: () => batchLoadState.cancelRequested,
      onProgress: (stats) => {
        if (stats && typeof stats === "object") {
          if (typeof stats.totalRecordsFromApi === "number") {
            batchLoadState.currentRecordsFromApi = stats.totalRecordsFromApi;
          }
          if (typeof stats.totalLoadedToDb === "number") {
            batchLoadState.currentLoadedToDb = stats.totalLoadedToDb;
          }
          if (typeof stats.totalSkippedNoKey === "number") {
            batchLoadState.currentSkippedNoKey = stats.totalSkippedNoKey;
          }
        }
      }
    });

    batchLoadState.lastError = null;
    if (totals && typeof totals === "object") {
      if (typeof totals.recordsFromApi === "number") {
        batchLoadState.currentRecordsFromApi = totals.recordsFromApi;
      }
      if (typeof totals.loadedToDb === "number") {
        batchLoadState.currentLoadedToDb = totals.loadedToDb;
      }
      if (typeof totals.skippedNoKey === "number") {
        batchLoadState.currentSkippedNoKey = totals.skippedNoKey;
      }
    }
    batchLoadState.lastRecordsFromApi = batchLoadState.currentRecordsFromApi;
    batchLoadState.lastLoadedToDb = batchLoadState.currentLoadedToDb;
    batchLoadState.lastSkippedNoKey = batchLoadState.currentSkippedNoKey;
    return "completed";
  } catch (err) {
    if (isAbortError(err)) {
      batchLoadState.lastError = null;
      batchLoadState.lastCancelledAtIso = new Date().toISOString();
      batchLoadState.lastRecordsFromApi = batchLoadState.currentRecordsFromApi;
      batchLoadState.lastLoadedToDb = batchLoadState.currentLoadedToDb;
      batchLoadState.lastSkippedNoKey = batchLoadState.currentSkippedNoKey;
      return "cancelled";
    }
    batchLoadState.lastError = err?.message || String(err);
    batchLoadState.lastRecordsFromApi = batchLoadState.currentRecordsFromApi;
    batchLoadState.lastLoadedToDb = batchLoadState.currentLoadedToDb;
    batchLoadState.lastSkippedNoKey = batchLoadState.currentSkippedNoKey;
    throw err;
  } finally {
    batchLoadState.cancelRequested = false;
    batchLoadState.abortController = null;
    batchLoadState.isRunning = false;
    batchLoadState.lastRunFinishedAtIso = new Date().toISOString();
  }
}

function requestBatchLoadCancellation() {
  if (!batchLoadState.isRunning) {
    return { accepted: false, reason: "not_running" };
  }
  if (batchLoadState.cancelRequested) {
    return { accepted: true, reason: "already_requested" };
  }
  batchLoadState.cancelRequested = true;
  batchLoadState.lastCancelRequestedAtIso = new Date().toISOString();
  const controller = batchLoadState.abortController;
  if (controller) {
    controller.abort();
  }
  return { accepted: true, reason: "requested" };
}

async function safeRunLeaseLoad(pg, options = {}) {
  const reference = typeof options.reference === "string" && options.reference.trim() !== ""
    ? options.reference.trim()
    : getLeaseReportRef();
  if (leaseLoadState.isRunning) {
    console.log(`[${reference}] Lease amortization sync already running; skipping new trigger.`);
    return "skipped";
  }

  leaseLoadState.isRunning = true;
  leaseLoadState.cancelRequested = false;
  const controller = new AbortController();
  leaseLoadState.abortController = controller;
  leaseLoadState.lastRunStartedAtIso = new Date().toISOString();
  leaseLoadState.lastRequestedAtIso = leaseLoadState.lastRunStartedAtIso;
  leaseLoadState.lastError = null;
  leaseLoadState.currentSummary = null;

  const verbose = typeof options.verbose === "boolean"
    ? options.verbose
    : Boolean(options.verbose ?? argv.verbose);

  try {
    const summary = await harvestLeaseAmortizationItems(pg, verbose, {
      signal: controller.signal,
      shouldCancel: () => leaseLoadState.cancelRequested
    });
    summary.startedAt = leaseLoadState.lastRunStartedAtIso;
    summary.completedAt = new Date().toISOString();
    leaseLoadState.lastSummary = summary;
    leaseLoadState.currentSummary = null;
    leaseLoadState.lastRunFinishedAtIso = summary.completedAt;
    return "completed";
  } catch (err) {
    if (isAbortError(err)) {
      leaseLoadState.lastError = null;
      leaseLoadState.lastCancelledAtIso = new Date().toISOString();
      leaseLoadState.lastRunFinishedAtIso = leaseLoadState.lastCancelledAtIso;
      if (leaseLoadState.currentSummary) {
        leaseLoadState.lastSummary = leaseLoadState.currentSummary;
      }
      leaseLoadState.currentSummary = null;
      return "cancelled";
    }
    leaseLoadState.lastError = err?.message || String(err);
    leaseLoadState.lastRunFinishedAtIso = new Date().toISOString();
    if (leaseLoadState.currentSummary) {
      leaseLoadState.lastSummary = leaseLoadState.currentSummary;
    }
    leaseLoadState.currentSummary = null;
    throw err;
  } finally {
    leaseLoadState.cancelRequested = false;
    leaseLoadState.abortController = null;
    leaseLoadState.isRunning = false;
  }
}

function requestLeaseLoadCancellation() {
  if (!leaseLoadState.isRunning) {
    return { accepted: false, reason: "not_running" };
  }
  if (leaseLoadState.cancelRequested) {
    return { accepted: true, reason: "already_requested" };
  }
  leaseLoadState.cancelRequested = true;
  leaseLoadState.lastCancelRequestedAtIso = new Date().toISOString();
  const controller = leaseLoadState.abortController;
  if (controller) {
    controller.abort();
  }
  return { accepted: true, reason: "requested" };
}

async function safeRunRegionLoad(pg, options = {}) {
  const reference = typeof options.reference === "string" && options.reference.trim() !== ""
    ? options.reference.trim()
    : getRegionReportRef();
  if (regionLoadState.isRunning) {
    console.log(`[${reference}] Region load already running; skipping new trigger.`);
    return "skipped";
  }

  regionLoadState.isRunning = true;
  regionLoadState.cancelRequested = false;
  const controller = new AbortController();
  regionLoadState.abortController = controller;
  regionLoadState.lastRunStartedAtIso = new Date().toISOString();
  regionLoadState.lastRequestedAtIso = options.requestedAt ?? regionLoadState.lastRunStartedAtIso;
  regionLoadState.lastError = null;
  regionLoadState.currentSummary = null;
  regionLoadState.currentRecordsFromApi = 0;
  regionLoadState.currentLoadedToDb = 0;
  regionLoadState.currentSkippedNoKey = 0;

  const startIso = options.start ?? getBatchStartIso();
  const endIso = options.end ?? getBatchEndIso();
  regionLoadState.lastStartIso = startIso;
  regionLoadState.lastEndIso = endIso;

  try {
    const rangeStart = parseRangeBoundary("region start", startIso, getBatchStartIso());
    const rangeEnd = parseRangeBoundary("region end", endIso, getBatchEndIso());
    const verbose = typeof options.verbose === "boolean"
      ? options.verbose
      : Boolean(options.verbose ?? argv.verbose);

    let summary = {
      reference,
      start: rangeStart.toISO(),
      end: rangeEnd.toISO(),
      recordsFromApi: 0,
      loadedToDb: 0,
      skippedNoKey: 0,
      startedAt: regionLoadState.lastRunStartedAtIso,
      completedAt: null
    };
    regionLoadState.currentSummary = { ...summary };

    const totals = await processDailyRange(pg, rangeStart, rangeEnd, verbose, {
      label: options.label ?? "RegionLoad",
      reference,
      destTable: REGION_TABLE,
      fieldStart: options.fieldStart ?? argv["field-start"],
      fieldEnd: options.fieldEnd ?? argv["field-end"],
      dump: options.dump ?? argv.dump,
      logHttp: options.logHttp ?? argv["log-http"],
      signal: controller.signal,
      shouldCancel: () => regionLoadState.cancelRequested,
      onProgress: (stats) => {
        if (!stats || typeof stats !== "object") return;
        if (typeof stats.totalRecordsFromApi === "number") {
          regionLoadState.currentRecordsFromApi = stats.totalRecordsFromApi;
          summary.recordsFromApi = stats.totalRecordsFromApi;
        }
        if (typeof stats.totalLoadedToDb === "number") {
          regionLoadState.currentLoadedToDb = stats.totalLoadedToDb;
          summary.loadedToDb = stats.totalLoadedToDb;
        }
        if (typeof stats.totalSkippedNoKey === "number") {
          regionLoadState.currentSkippedNoKey = stats.totalSkippedNoKey;
          summary.skippedNoKey = stats.totalSkippedNoKey;
        }
        regionLoadState.currentSummary = { ...summary };
      }
    });

    if (totals && typeof totals === "object") {
      if (typeof totals.recordsFromApi === "number") {
        regionLoadState.currentRecordsFromApi = totals.recordsFromApi;
        summary.recordsFromApi = totals.recordsFromApi;
      }
      if (typeof totals.loadedToDb === "number") {
        regionLoadState.currentLoadedToDb = totals.loadedToDb;
        summary.loadedToDb = totals.loadedToDb;
      }
      if (typeof totals.skippedNoKey === "number") {
        regionLoadState.currentSkippedNoKey = totals.skippedNoKey;
        summary.skippedNoKey = totals.skippedNoKey;
      }
    }

    summary.completedAt = new Date().toISOString();
    regionLoadState.lastSummary = summary;
    regionLoadState.currentSummary = null;
    regionLoadState.lastRunFinishedAtIso = summary.completedAt;
    regionLoadState.lastRecordsFromApi = regionLoadState.currentRecordsFromApi;
    regionLoadState.lastLoadedToDb = regionLoadState.currentLoadedToDb;
    regionLoadState.lastSkippedNoKey = regionLoadState.currentSkippedNoKey;
    return "completed";
  } catch (err) {
    if (isAbortError(err)) {
      regionLoadState.lastError = null;
      regionLoadState.lastCancelledAtIso = new Date().toISOString();
      regionLoadState.lastRunFinishedAtIso = regionLoadState.lastCancelledAtIso;
      if (regionLoadState.currentSummary) {
        regionLoadState.lastSummary = {
          ...regionLoadState.currentSummary,
          completedAt: regionLoadState.lastCancelledAtIso
        };
      }
      regionLoadState.lastRecordsFromApi = regionLoadState.currentRecordsFromApi;
      regionLoadState.lastLoadedToDb = regionLoadState.currentLoadedToDb;
      regionLoadState.lastSkippedNoKey = regionLoadState.currentSkippedNoKey;
      return "cancelled";
    }
    regionLoadState.lastError = err?.message || String(err);
    regionLoadState.lastRunFinishedAtIso = new Date().toISOString();
    if (regionLoadState.currentSummary) {
      regionLoadState.lastSummary = regionLoadState.currentSummary;
    }
    regionLoadState.lastRecordsFromApi = regionLoadState.currentRecordsFromApi;
    regionLoadState.lastLoadedToDb = regionLoadState.currentLoadedToDb;
    regionLoadState.lastSkippedNoKey = regionLoadState.currentSkippedNoKey;
    throw err;
  } finally {
    regionLoadState.cancelRequested = false;
    regionLoadState.abortController = null;
    regionLoadState.isRunning = false;
  }
}

function requestRegionLoadCancellation() {
  if (!regionLoadState.isRunning) {
    return { accepted: false, reason: "not_running" };
  }
  if (regionLoadState.cancelRequested) {
    return { accepted: true, reason: "already_requested" };
  }
  regionLoadState.cancelRequested = true;
  regionLoadState.lastCancelRequestedAtIso = new Date().toISOString();
  const controller = regionLoadState.abortController;
  if (controller) {
    controller.abort();
  }
  return { accepted: true, reason: "requested" };
}

async function backfillThenSchedule(pg, verbose) {
  // 1-day windows, 30 at a time, from 2025-09-06 -> 2350-01-01 (override with --from/--to)
  const bfStart = parseRangeBoundary("backfill start", argv.from ?? getBatchStartIso(), getBatchStartIso());
  const bfEnd = parseRangeBoundary("backfill end", argv.to ?? getBatchEndIso(), getBatchEndIso());

  await processDailyRange(pg, bfStart, bfEnd, verbose, {
    label: "Backfill",
    reference: getBatchReportRef()
  });

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
  const publicDir = path.join(process.cwd(), "public");
  if (fs.existsSync(publicDir)) {
    app.use(express.static(publicDir));
  }

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

  const extractReference = (req, fallback) => {
    const param = req.params?.reference;
    if (typeof param === "string" && param.trim() !== "") return param.trim();
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

    const refreshReference = getRefreshReportRef();

    return {
      refreshReference,
      reference: refreshReference,
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
      cancelRequested: ptrRep788State.cancelRequested,
      lastCancelRequestedAt: ptrRep788State.lastCancelRequestedAtIso,
      lastCancelledAt: ptrRep788State.lastCancelledAtIso,
      currentActivityId: ptrRep788State.currentActivityId,
      lastError: ptrRep788State.lastError,
      preferredVerbose: ptrRep788State.preferredVerbose,
      minutesPerSweep: PTR_REP_788_SWEEP_MINUTES,
      delayPerCallMs: ptrRep788State.delayPerCallMs,
      lastCatchupRequestedAt: ptrRep788State.lastCatchupRequestedAtIso,
      lastCatchupCompletedAt: ptrRep788State.lastCatchupCompletedAtIso,
      lastCatchupDays: ptrRep788State.lastCatchupDays,
      currentRecordsFromApi: ptrRep788State.currentRecordsFromApi,
      currentLoadedToDb: ptrRep788State.currentLoadedToDb,
      currentSkippedNoKey: ptrRep788State.currentSkippedNoKey,
      lastRecordsFromApi: ptrRep788State.lastRecordsFromApi,
      lastLoadedToDb: ptrRep788State.lastLoadedToDb,
      lastSkippedNoKey: ptrRep788State.lastSkippedNoKey,
      batchLoad: {
        reference: getBatchReportRef(),
        isRunning: batchLoadState.isRunning,
        cancelRequested: batchLoadState.cancelRequested,
        lastRequestedAt: batchLoadState.lastRequestedAtIso,
        lastRunStartedAt: batchLoadState.lastRunStartedAtIso,
        lastRunFinishedAt: batchLoadState.lastRunFinishedAtIso,
        lastError: batchLoadState.lastError,
        lastStartIso: batchLoadState.lastStartIso,
        lastEndIso: batchLoadState.lastEndIso,
        lastCancelRequestedAt: batchLoadState.lastCancelRequestedAtIso,
        lastCancelledAt: batchLoadState.lastCancelledAtIso,
        currentActivityId: batchLoadState.currentActivityId,
        currentRecordsFromApi: batchLoadState.currentRecordsFromApi,
        currentLoadedToDb: batchLoadState.currentLoadedToDb,
        currentSkippedNoKey: batchLoadState.currentSkippedNoKey,
        lastRecordsFromApi: batchLoadState.lastRecordsFromApi,
        lastLoadedToDb: batchLoadState.lastLoadedToDb,
        lastSkippedNoKey: batchLoadState.lastSkippedNoKey
      },
      leaseLoad: {
        reference: getLeaseReportRef(),
        isRunning: leaseLoadState.isRunning,
        cancelRequested: leaseLoadState.cancelRequested,
        lastRequestedAt: leaseLoadState.lastRequestedAtIso,
        lastRunStartedAt: leaseLoadState.lastRunStartedAtIso,
        lastRunFinishedAt: leaseLoadState.lastRunFinishedAtIso,
        lastError: leaseLoadState.lastError,
        lastCancelRequestedAt: leaseLoadState.lastCancelRequestedAtIso,
        lastCancelledAt: leaseLoadState.lastCancelledAtIso,
        currentActivityId: leaseLoadState.currentActivityId,
        summary: leaseLoadState.currentSummary ?? leaseLoadState.lastSummary,
        currentSummary: leaseLoadState.currentSummary,
        lastSummary: leaseLoadState.lastSummary
      },
      regionLoad: {
        reference: getRegionReportRef(),
        isRunning: regionLoadState.isRunning,
        cancelRequested: regionLoadState.cancelRequested,
        lastRequestedAt: regionLoadState.lastRequestedAtIso,
        lastRunStartedAt: regionLoadState.lastRunStartedAtIso,
        lastRunFinishedAt: regionLoadState.lastRunFinishedAtIso,
        lastError: regionLoadState.lastError,
        lastStartIso: regionLoadState.lastStartIso,
        lastEndIso: regionLoadState.lastEndIso,
        lastCancelRequestedAt: regionLoadState.lastCancelRequestedAtIso,
        lastCancelledAt: regionLoadState.lastCancelledAtIso,
        currentActivityId: regionLoadState.currentActivityId,
        summary: regionLoadState.currentSummary ?? regionLoadState.lastSummary,
        currentSummary: regionLoadState.currentSummary,
        lastSummary: regionLoadState.lastSummary,
        currentRecordsFromApi: regionLoadState.currentRecordsFromApi,
        currentLoadedToDb: regionLoadState.currentLoadedToDb,
        currentSkippedNoKey: regionLoadState.currentSkippedNoKey,
        lastRecordsFromApi: regionLoadState.lastRecordsFromApi,
        lastLoadedToDb: regionLoadState.lastLoadedToDb,
        lastSkippedNoKey: regionLoadState.lastSkippedNoKey
      }
    };
  };

  app.get("/api/config", (_req, res) => {
    res.json(getConfigSnapshot());
  });

  app.post("/api/config", (req, res) => {
    if (req?.body && typeof req.body === "object") {
      updateConfig(req.body);
    }
    res.json(getConfigSnapshot());
  });

  app.get("/api/activity", (_req, res) => {
    res.json({ items: activityLog });
  });

  const leaseLoadHandler = (req, res) => {
    const reference = extractReference(req, getLeaseReportRef());
    const nowIso = new Date().toISOString();
    leaseLoadState.lastRequestedAtIso = nowIso;

    const verbose = coerceBoolean(req.body?.verbose ?? req.query?.verbose, argv.verbose);

    const activity = createActivityEntry({
      functionName: "Lease Load",
      reportReference: reference
    });
    if (!leaseLoadState.isRunning) {
      leaseLoadState.currentActivityId = activity.id;
    }

    safeRunLeaseLoad(pg, { reference, verbose })
      .then((result) => {
        const completedAt = new Date().toISOString();
        const status = result === "completed" ? "completed" : result === "cancelled" ? "cancelled" : "skipped";
        updateActivityEntry(activity.id, {
          status,
          completedAt
        });
      })
      .catch((err) => {
        const message = err?.message || String(err);
        updateActivityEntry(activity.id, {
          status: "failed",
          completedAt: new Date().toISOString(),
          error: message
        });
      })
      .finally(() => {
        if (leaseLoadState.currentActivityId === activity.id) leaseLoadState.currentActivityId = null;
      });

    const status = buildStatus();
    status.leaseReference = reference;

    res.status(202).json({
      activityId: activity.id,
      reference,
      summary: leaseLoadState.currentSummary ?? leaseLoadState.lastSummary,
      status
    });
  };

  const cancelLeaseHandler = (req, res) => {
    const reference = extractReference(req, getLeaseReportRef());
    const result = requestLeaseLoadCancellation();
    if (!result.accepted) {
      return res.status(409).json({
        cancelled: false,
        reason: result.reason,
        status: buildStatus()
      });
    }

    if (leaseLoadState.currentActivityId) {
      const entry = updateActivityEntry(leaseLoadState.currentActivityId, {
        status: "cancelling"
      });
      if (entry && entry.status === "cancelling") {
        entry.completedAt = null;
      }
    }

    const status = buildStatus();
    status.leaseReference = reference;

    res.status(202).json({
      cancelled: true,
      reason: result.reason,
      reference,
      status
    });
  };

  const regionLoadHandler = (req, res) => {
    const reference = extractReference(req, getRegionReportRef());
    const nowIso = new Date().toISOString();
    regionLoadState.lastRequestedAtIso = nowIso;

    const startIso = req.body?.start ?? req.query?.start ?? getBatchStartIso();
    const endIso = req.body?.end ?? req.query?.end ?? getBatchEndIso();
    const verbose = coerceBoolean(req.body?.verbose ?? req.query?.verbose, argv.verbose);

    const activity = createActivityEntry({
      functionName: "PT Region Load",
      reportReference: reference
    });
    if (!regionLoadState.isRunning) {
      regionLoadState.currentActivityId = activity.id;
    }

    safeRunRegionLoad(pg, { reference, start: startIso, end: endIso, verbose, requestedAt: nowIso })
      .then((result) => {
        const completedAt = new Date().toISOString();
        const statusLabel = result === "completed" ? "completed" : result === "cancelled" ? "cancelled" : "skipped";
        updateActivityEntry(activity.id, {
          status: statusLabel,
          completedAt
        });
      })
      .catch((err) => {
        const message = err?.message || String(err);
        updateActivityEntry(activity.id, {
          status: "failed",
          completedAt: new Date().toISOString(),
          error: message
        });
      })
      .finally(() => {
        if (regionLoadState.currentActivityId === activity.id) regionLoadState.currentActivityId = null;
      });

    const status = buildStatus();
    status.regionLoadReference = reference;

    res.status(202).json({
      activityId: activity.id,
      reference,
      start: startIso,
      end: endIso,
      summary: regionLoadState.currentSummary ?? regionLoadState.lastSummary,
      status
    });
  };

  const cancelRegionHandler = (req, res) => {
    const reference = extractReference(req, getRegionReportRef());
    const result = requestRegionLoadCancellation();
    if (!result.accepted) {
      return res.status(409).json({
        cancelled: false,
        reason: result.reason,
        status: buildStatus()
      });
    }

    if (regionLoadState.currentActivityId) {
      const entry = updateActivityEntry(regionLoadState.currentActivityId, {
        status: "cancelling"
      });
      if (entry && entry.status === "cancelling") {
        entry.completedAt = null;
      }
    }

    const status = buildStatus();
    status.regionLoadReference = reference;

    res.status(202).json({
      cancelled: true,
      reason: result.reason,
      reference,
      status
    });
  };

  app.post("/api/lease-amortization/run", leaseLoadHandler);
  app.post("/api/lease-amortization/cancel", cancelLeaseHandler);
  app.post("/api/reports/:reference/lease-amortization", leaseLoadHandler);
  app.post("/api/reports/:reference/lease-amortization/cancel", cancelLeaseHandler);
  app.post("/api/reports/lease-amortization", leaseLoadHandler);
  app.post("/api/reports/lease-amortization/cancel", cancelLeaseHandler);
  app.post("/api/ptregion/run", regionLoadHandler);
  app.post("/api/ptregion/cancel", cancelRegionHandler);
  app.post("/api/reports/:reference/ptregion", regionLoadHandler);
  app.post("/api/reports/:reference/ptregion/cancel", cancelRegionHandler);
  app.post("/api/reports/ptregion", regionLoadHandler);
  app.post("/api/reports/ptregion/cancel", cancelRegionHandler);

  const refreshHandler = (req, res) => {
    const reference = extractReference(req, getRefreshReportRef());
    const nowIso = new Date().toISOString();
    ptrRep788State.lastRequestedAtIso = nowIso;

    if (req?.body && typeof req.body === "object" && Object.prototype.hasOwnProperty.call(req.body, "verbose")) {
      ptrRep788State.preferredVerbose = coerceBoolean(req.body.verbose, ptrRep788State.preferredVerbose);
    }
    if (Object.prototype.hasOwnProperty.call(req.query ?? {}, "verbose")) {
      ptrRep788State.preferredVerbose = coerceBoolean(req.query.verbose, ptrRep788State.preferredVerbose);
    }

    const delayCandidate = req.body?.delayMs ?? req.query?.delayMs;
    let delayOverride;
    if (typeof delayCandidate !== "undefined") {
      const parsedDelay = Number(delayCandidate);
      if (Number.isFinite(parsedDelay) && parsedDelay >= 0) {
        ptrRep788State.delayPerCallMs = parsedDelay;
        delayOverride = parsedDelay;
      }
    }

    const job = ensurePtrRep788Job(pg);
    const jobWasRunning = job.running;
    if (!job.running) job.start();

    const activity = createActivityEntry({
      functionName: "15-minute sweep",
      reportReference: reference
    });
    if (!ptrRep788State.isTickRunning) {
      ptrRep788State.currentActivityId = activity.id;
    }

    safeRunPtrRep788Sweep(pg, ptrRep788State.preferredVerbose, {
      reference,
      delayMs: delayOverride
    })
      .then((result) => {
        const completedAt = new Date().toISOString();
        const status = result === "completed" ? "completed" : result === "cancelled" ? "cancelled" : "skipped";
        updateActivityEntry(activity.id, {
          status,
          completedAt
        });
      })
      .catch((err) => {
        const message = err?.message || String(err);
        ptrRep788State.lastError = message;
        updateActivityEntry(activity.id, {
          status: "failed",
          completedAt: new Date().toISOString(),
          error: message
        });
      })
      .finally(() => {
        if (ptrRep788State.currentActivityId === activity.id) ptrRep788State.currentActivityId = null;
      });

    const status = buildStatus();
    status.requestedReference = reference;
    status.jobWasRunning = jobWasRunning;

    res.status(jobWasRunning ? 202 : 201).json({
      activityId: activity.id,
      reference,
      jobWasRunning,
      status
    });
  };

  const catchupHandler = (req, res) => {
    const reference = extractReference(req, getRefreshReportRef());
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

    const delayCandidate = req.body?.delayMs ?? req.query?.delayMs;
    let delayMsOverride;
    if (typeof delayCandidate !== "undefined") {
      const parsedDelay = Number(delayCandidate);
      if (Number.isFinite(parsedDelay) && parsedDelay >= 0) {
        ptrRep788State.delayPerCallMs = parsedDelay;
        delayMsOverride = parsedDelay;
      }
    }

    const totalMinutes = Math.ceil(days * 24 * 60);
    if (!Number.isFinite(totalMinutes) || totalMinutes <= 0) {
      return res.status(400).json({
        error: "Calculated minutes to process is invalid.",
        days,
        totalMinutes
      });
    }

    const job = ensurePtrRep788Job(pg);
    const jobWasRunning = job.running;
    if (!job.running) job.start();

    const activity = createActivityEntry({
      functionName: `Catchup (${days}d)`,
      reportReference: reference
    });
    if (!ptrRep788State.isTickRunning) {
      ptrRep788State.currentActivityId = activity.id;
    }

    const sweepOptions = {
      minutes: totalMinutes,
      pivot: DateTime.now().setZone(TZ),
      label: `${reference} catchup(${days}d)`,
      reference
    };
    if (delayMsOverride != null) sweepOptions.delayMs = delayMsOverride;

    safeRunPtrRep788Sweep(pg, ptrRep788State.preferredVerbose, sweepOptions)
      .then((result) => {
        const completedAt = new Date().toISOString();
        if (result === "completed") {
          ptrRep788State.lastCatchupCompletedAtIso = completedAt;
        }
        const status = result === "completed" ? "completed" : result === "cancelled" ? "cancelled" : "skipped";
        updateActivityEntry(activity.id, {
          status,
          completedAt
        });
      })
      .catch((err) => {
        const message = err?.message || String(err);
        ptrRep788State.lastError = message;
        updateActivityEntry(activity.id, {
          status: "failed",
          completedAt: new Date().toISOString(),
          error: message
        });
      })
      .finally(() => {
        if (ptrRep788State.currentActivityId === activity.id) ptrRep788State.currentActivityId = null;
      });

    const status = buildStatus();
    status.catchupRequestedDays = days;
    status.minutesToProcess = totalMinutes;
    status.requestedReference = reference;

    res.status(202).json({
      activityId: activity.id,
      reference,
      days,
      status
    });
  };

  const batchloadHandler = (req, res) => {
    const reference = extractReference(req, getBatchReportRef());
    const nowIso = new Date().toISOString();
    batchLoadState.lastRequestedAtIso = nowIso;

    const startIso = req.body?.start ?? req.query?.start ?? getBatchStartIso();
    const endIso = req.body?.end ?? req.query?.end ?? getBatchEndIso();
    const verbose = coerceBoolean(req.body?.verbose ?? req.query?.verbose, argv.verbose);

    const activity = createActivityEntry({
      functionName: "Batch Load",
      reportReference: reference
    });
    if (!batchLoadState.isRunning) {
      batchLoadState.currentActivityId = activity.id;
    }

    safeRunBatchLoad(pg, { reference, start: startIso, end: endIso, verbose })
      .then((result) => {
        const completedAt = new Date().toISOString();
        const status = result === "completed" ? "completed" : result === "cancelled" ? "cancelled" : "skipped";
        updateActivityEntry(activity.id, {
          status,
          completedAt
        });
      })
      .catch((err) => {
        const message = err?.message || String(err);
        updateActivityEntry(activity.id, {
          status: "failed",
          completedAt: new Date().toISOString(),
          error: message
        });
      })
      .finally(() => {
        if (batchLoadState.currentActivityId === activity.id) batchLoadState.currentActivityId = null;
      });

    const status = buildStatus();
    status.batchLoadRequestedStart = startIso;
    status.batchLoadRequestedEnd = endIso;
    status.batchLoadReference = reference;

    res.status(202).json({
      activityId: activity.id,
      reference,
      start: startIso,
      end: endIso,
      status
    });
  };

  const cancelSweepHandler = (req, res) => {
    const reference = extractReference(req, getRefreshReportRef());
    const result = requestPtrRep788Cancellation();
    if (!result.accepted) {
      return res.status(409).json({
        cancelled: false,
        reason: result.reason,
        status: buildStatus()
      });
    }

    if (ptrRep788State.currentActivityId) {
      const entry = updateActivityEntry(ptrRep788State.currentActivityId, {
        status: "cancelling"
      });
      if (entry && entry.status === "cancelling") {
        entry.completedAt = null;
      }
    }

    const status = buildStatus();
    status.reference = reference;
    res.status(202).json({
      cancelled: true,
      reason: result.reason,
      reference,
      status
    });
  };

  const cancelBatchHandler = (req, res) => {
    const reference = extractReference(req, getBatchReportRef());
    const result = requestBatchLoadCancellation();
    if (!result.accepted) {
      return res.status(409).json({
        cancelled: false,
        reason: result.reason,
        status: buildStatus()
      });
    }

    if (batchLoadState.currentActivityId) {
      const entry = updateActivityEntry(batchLoadState.currentActivityId, {
        status: "cancelling"
      });
      if (entry && entry.status === "cancelling") {
        entry.completedAt = null;
      }
    }

    const status = buildStatus();
    status.batchLoadReference = reference;
    res.status(202).json({
      cancelled: true,
      reason: result.reason,
      reference,
      status
    });
  };

  app.post("/api/reports/:reference/refresh", refreshHandler);
  app.post("/api/reports/:reference/catchup", catchupHandler);
  app.post("/api/reports/:reference/batchload", batchloadHandler);
  app.post("/api/reports/:reference/cancel", cancelSweepHandler);
  app.post("/api/reports/:reference/batchcancel", cancelBatchHandler);
  app.get("/api/reports/:reference/status", (_req, res) => {
    res.json(buildStatus());
  });

  // Legacy fallback routes without explicit reference
  app.post("/api/reports/refresh", refreshHandler);
  app.post("/api/reports/catchup", catchupHandler);
  app.post("/api/reports/batchload", batchloadHandler);
  app.post("/api/reports/cancel", cancelSweepHandler);
  app.post("/api/reports/batchcancel", cancelBatchHandler);
  app.get("/api/reports/status", (_req, res) => {
    res.json(buildStatus());
  });

  app.get("/api/status", (_req, res) => {
    res.json(buildStatus());
  });

  ptrRep788State.preferredVerbose = coerceBoolean(defaultVerbose, true);

  await new Promise((resolve) => {
    app.listen(port, () => {
      const refreshPath = `/api/reports/${getRefreshReportRef()}/refresh`;
      const statusPath = `/api/reports/${getRefreshReportRef()}/status`;
      console.log(`HTTP server listening on port ${port}`);
      console.log(`POST ${refreshPath} to run the 15-minute sweep.`);
      console.log(`GET  ${statusPath} to inspect scheduler state.`);
      resolve();
    });
  });
}

/** ===== Main ===== */
(async () => {
  const pg = new Client(PG);
  await pg.connect();
  await ensureTable(pg);
  await ensureTable(pg, LEASE_ITEMS_TABLE);
  await ensureTable(pg, REGION_TABLE);

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
