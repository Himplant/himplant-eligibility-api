import "dotenv/config";

// Safety shim: the persistent lock wrapper was disabled after it blocked live lead capture
// when the Supabase RPC layer failed closed. Keep this file as a safe passthrough so
// a stale Render manual start command like `node server-with-lock.js` still runs the
// production API instead of intercepting /api/submissions.
//
// Do not reintroduce a submission lock here unless it is strictly fail-open:
// lock/RPC failure must log and forward to the normal Zoho submission handler.
console.warn("server-with-lock.js is disabled; starting direct server.js path.");
await import("./server.js");
