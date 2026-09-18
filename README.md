# himplant-eligibility-api

## Secure app prefill handoff (SCRUM-23)

`POST /api/handoff/exchange` with `{ "code": "<43-char one-time code>" }` redeems a code created by the Himplant app and returns `{ prefill: { first_name, last_name, email, phone } }`. Codes are single-use and expire after five minutes. Responses: `404 invalid_or_expired`, `400 invalid_code`, `429 rate_limited` (20 per IP per 10 minutes), `502/503 unavailable`.

The server redeems the code with the app's Supabase `eligibility-handoff` function using the existing `SUPABASE_ELIGIBILITY_API_KEY` and `SUPABASE_ANON_KEY`. Optional `ELIGIBILITY_HANDOFF_URL` overrides the function URL; by default it is derived from `SUPABASE_FUNCTION_URL`. Codes and request bodies are never logged.
