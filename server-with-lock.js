import "dotenv/config";
import express from "express";
import cors from "cors";
import fetch from "node-fetch";
import crypto from "crypto";

const originalPort = process.env.PORT || "10000";
const externalPort = Number(originalPort) || 10000;
const internalPort = Number(process.env.INTERNAL_SERVER_PORT || externalPort + 1);
process.env.PORT = String(internalPort);
await import("./server.js");
process.env.PORT = originalPort;

const ALLOWED_ORIGINS = [
  "https://eligibility.himplant.com",
  "https://himplant.com",
  "https://www.himplant.com",
  "https://get.himplant.com",
  "https://lovableproject.com",
  "https://lovable.dev",
  "https://bc248be2-fa03-4ded-a845-6db79ac12fb7.lovableproject.com",
  "https://himplanteligibility.lovable.app",
  "http://localhost:8080",
  "http://localhost:3000",
  "http://localhost:5173",
  "http://127.0.0.1:8080",
  "http://127.0.0.1:3000",
  "http://127.0.0.1:5173",
];

const app = express();
app.use(
  cors({
    origin(origin, callback) {
      if (!origin) return callback(null, true);
      if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
      if (origin.startsWith("http://localhost:") || origin.startsWith("http://127.0.0.1:")) {
        return callback(null, true);
      }
      return callback(new Error("Not allowed by CORS"));
    },
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);
app.options("*", cors());
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
  const response = await fetch(`${SUPAB