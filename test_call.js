import axios from "axios";

const AX_URL = "https://ptrac.axxerion.us/webservices/ptrac/rest/functions/completereportresult";
const AUTH = "Basic YXh1cHRyYWM6UXJlcG9pITU1"; // yours
const body = {
  reference: "PTR-REP-783",
  filterFields: ["startDate", "endDate"],
  filterValues: ["1/1/2025 00:00 AM", "1/2/2025 00:00 AM"]
};

(async () => {
  console.log("=== REQUEST ===");
  console.log("POST", AX_URL);
  console.log("Headers:", { Authorization: AUTH, "Content-Type": "text/plain" });
  console.log("Body (stringified):", JSON.stringify(body));

  try {
    const resp = await axios.post(AX_URL, JSON.stringify(body), {
      headers: { Authorization: AUTH, "Content-Type": "text/plain" },
      timeout: 1800000,
      validateStatus: () => true // don't throw on non-2xx
    });

    console.log("\n=== RESPONSE ===");
    console.log("Status:", resp.status);
    console.log("StatusText:", resp.statusText);
    console.log("Headers:", resp.headers);

    let raw = resp.data;
    if (typeof raw !== "string") raw = JSON.stringify(raw);
    console.log("\nBody (first 2000 chars):");
    console.log(String(raw).slice(0, 2000));

    // Try to parse / normalize
    let parsed = resp.data;
    try { if (typeof parsed === "string") parsed = JSON.parse(parsed); } catch {}
    let rows = [];
    if (Array.isArray(parsed)) rows = parsed;
    else if (parsed?.rows) rows = parsed.rows;
    else if (parsed?.result) rows = parsed.result;
    else if (parsed?.data) rows = parsed.data;
    else if (parsed && typeof parsed === "object") rows = [parsed];

    console.log(`\nParsed rows: ${rows.length}`);
    if (rows.length) {
      console.log("Sample row keys:", Object.keys(rows[0]).slice(0, 25));
      console.log("Sample row:", JSON.stringify(rows[0], null, 2).slice(0, 1000));
    }
  } catch (e) {
    console.error("\nERROR calling API:", e.message);
    if (e.response) {
      console.error("Resp status:", e.response.status);
      console.error("Resp headers:", e.response.headers);
      console.error("Resp data:", typeof e.response.data === "string" ? e.response.data : JSON.stringify(e.response.data));
    }
  }
})();
