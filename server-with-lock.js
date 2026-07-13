import "dotenv/config";
import express from "express";
import fetch from "node-fetch";
import crypto from "crypto";

const originalPort = process.env.PORT || "10000";
const externalPort = Number(originalPort) || 10000;
const internalPort = Number(process.env.INTERNAL_SERVER_PORT || externalPort + 1);
process.env.PORT = String(internalPort);
await import("./server.js");
process.env.PORT = originalPort;

const app = express();
app.use(express.json({ limit: "5mb" }));

const DEFAULT_SUPABASE_FUNCTION_URL = "https://nfoeswlppebvxaomfnsk.supabase.co/functions/v1/eligibility-complete";
const SUPABASE_BASE_URL = getSupabaseBaseUrl();
const SUPABASE_KEY = process.env.SUPABASE_ANON_KEY || "";
const LOCK_TTL_SECONDS = Number(process.env.SUBMISSION_LOCK_TTL_SECONDS || 75);
const LOCK_WAIT_MS = Number(process.env.SUBMISSION_LOCK_WAIT_MS || 10000);
const LOCK_POLL_MS = Number(process.env.SUBMISSION_LOCK_POLL_MS || 500);

function digitsOnly(value) { return String(value || "").replace(/\D/g, ""); }
function normalizeEmail(value) {
  const s = String(value || "").trim().toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s) ? s : "";
}
function normalizePhoneE164(countryCodeOrDial, phoneNumber) {
  const raw = String(phoneNumber || "").trim();
  if (raw.startsWith("+")) return "+" + digitsOnly(raw);
  const pn = digitsOnly(raw);
  const dial = digitsOnly(countryCodeOrDial || "");
  if (!dial && !pn) return "";
  return dial ? `+${dial}${pn}` : `+${pn}`;
}
function sha256(value) {
  const s = String(value || "").trim();
  return s ? crypto.createHash("sha256").update(s).digest("hex") : null;
}
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
function getSupabaseBaseUrl() {
  const explicit = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL;
  if (explicit) return explicit.replace(/\/$/, "");
  try { return new URL(process.env.SUPABASE_FUNCTION_URL || DEFAULT_SUPABASE_FUNCTION_URL).origin; } catch { return ""; }
}
function lockReady() { return Boolean(SUPABASE_BASE_URL && SUPABASE_KEY); }
function lockPayload(submission) {
  const sessionId = String(submission.session_id || "").trim() || null;
  const email = normalizeEmail(submission.email);
  const phone = normalizePhoneE164(submission.phone_country_code, submission.phone_number);
  return {
    p_session_id: sessionId,
    p_email_hash: sha256(email),
    p_phone_hash: sha256(phone),
    p_idempotency_key: String(submission.idempotency_key || "").trim() || null,
    p_lock_ttl_seconds: LOCK_TTL_SECONDS,
  };
}
async function rpc(name, payload) {
  const response = await fetch(`${SUPABASE_BASE_URL}/rest/v1/rpc/${name}`, {
    method: "POST",
    headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(payload || {}),
  });
  const text = await response.text();
  let data;
  try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }
  if (!response.ok) {
    const error = new Error(`lock rpc ${name} failed: ${response.status}`);
    error.details = data;
    throw error;
  }
  return Array.isArray(data) ? data[0] : data;
}
async function acquireLock(submission) { return rpc("acquire_eligibility_lead_lock", lockPayload(submission)); }
async function resolveLock(submission) {
  const payload = lockPayload(submission);
  delete payload.p_lock_ttl_seconds;
  return rpc("resolve_eligibility_lead_lock", payload);
}
async function completeLock(submission, internalResult) {
  const payload = lockPayload(submission);
  delete payload.p_lock_ttl_seconds;
  return rpc("complete_eligibility_lead_lock", { ...payload, p_zoho_lead_id: internalResult?.body?.lead_id ? String(internalResult.body.lead_id) : null, p_last_response: internalResult?.body || null });
}
async function failLock(submission, message) {
  const payload = lockPayload(submission);
  delete payload.p_lock_ttl_seconds;
  try { await rpc("fail_eligibility_lead_lock", { ...payload, p_error_message: String(message || "submission failed") }); }
  catch (error) { console.error("[submission-lock] fail lock error:", error.message, error.details || ""); }
}
function cached(lockRow) {
  if (!lockRow?.last_response) return null;
  return { status: 200, body: { ...lockRow.last_response, duplicate_replay: true, persistent_lock: { status: lockRow.status, zoho_lead_id: lockRow.zoho_lead_id || lockRow.last_response?.lead_id || null } } };
}
async function waitForLock(submission) {
  const start = Date.now();
  while (Date.now() - start < LOCK_WAIT_MS) {
    await sleep(LOCK_POLL_MS);
    const row = await resolveLock(submission);
    if (row?.status === "completed" && row?.last_response) return cached(row);
    if (row?.status === "failed") return null;
  }
  return { status: 409, body: { success: false, error: "duplicate_submission_processing", message: "A matching submission is already being processed. Please retry shortly." } };
}
async function postInternal(submission) {
  const response = await fetch(`http://127.0.0.1:${internalPort}/api/submissions`, { method: "POST", headers: { "Content-Type": "application/json", Accept: "application/json" }, body: JSON.stringify(submission) });
  const text = await response.text();
  let body;
  try { body = text ? JSON.parse(text) : {}; } catch { body = { raw: text }; }
  return { status: response.status, body };
}
async function proxy(req, res) {
  const headers = { ...req.headers };
  delete headers.host; delete headers.connection; delete headers["content-length"];
  const hasBody = !["GET", "HEAD"].includes(req.method.toUpperCase());
  const response = await fetch(`http://127.0.0.1:${internalPort}${req.originalUrl}`, { method: req.method, headers, body: hasBody ? JSON.stringify(req.body || {}) : undefined });
  res.status(response.status);
  response.headers.forEach((value, key) => { if (!["content-encoding", "content-length", "transfer-encoding"].includes(key.toLowerCase())) res.setHeader(key, value); });
  res.send(Buffer.from(await response.arrayBuffer()));
}
function shouldLock(submission) {
  const type = String(submission?.submission_type || "").toLowerCase();
  return ["lead", "partial", "complete"].includes(type) && Boolean(String(submission?.session_id || "").trim());
}

app.post("/api/submissions", async (req, res) => {
  const submission = req.body || {};
  if (!shouldLock(submission) || !lockReady()) {
    if (!lockReady()) console.warn("[submission-lock] not configured; forwarding without persistent lock");
    const internal = await postInternal(submission);
    return res.status(internal.status).json(internal.body);
  }
  let acquired = false;
  try {
    const lock = await acquireLock(submission);
    acquired = Boolean(lock?.acquired);
    if (!acquired) {
      if (lock?.status === "completed" && lock?.last_response) {
        const hit = cached(lock);
        return res.status(hit.status).json(hit.body);
      }
      const resolved = await waitForLock(submission);
      if (resolved) return res.status(resolved.status).json(resolved.body);
      return res.status(409).json({ success: false, error: "submission_lock_unavailable" });
    }
    const internal = await postInternal(submission);
    if (internal.status >= 200 && internal.status < 300 && internal.body?.success !== false) await completeLock(submission, internal);
    else await failLock(submission, internal.body?.error || `internal status ${internal.status}`);
    return res.status(internal.status).json(internal.body);
  } catch (error) {
    console.error("[submission-lock] error:", error.message, error.details || "");
    if (acquired) await failLock(submission, error.message);
    return res.status(503).json({ success: false, error: "submission_lock_error", message: "Submission could not be safely processed. Please retry shortly." });
  }
});

app.use(async (req, res) => {
  try { await proxy(req, res); }
  catch (error) { console.error("[proxy] error:", error.message || error); res.status(502).json({ success: false, error: "proxy_failed" }); }
});

app.listen(externalPort, () => console.log(`API lock wrapper running on port ${externalPort}, proxying to ${internalPort}`));
