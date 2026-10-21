import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const read = (name) => fs.readFileSync(new URL('../' + name, import.meta.url), 'utf8');
const stripImports = (code) => code.replace(/^import .*;\n/gm, '');

function loadServer() {
  const app = { use() {}, get() {}, post() {}, listen() {} };
  const express = Object.assign(() => app, { json: () => () => {} });
  const context = vm.createContext({ express, cors: () => () => {}, process: { env: {} },
    console, URL, URLSearchParams, setTimeout, fetch: () => { throw Error('Unexpected network request'); } });
  vm.runInContext(stripImports(read('server.js')) + '\nglobalThis.mapping = { mapLeadBase, mapPartialBase, mapCompleteBase };', context);
  return context.mapping;
}

test('all submission types retain Portuguese and canonical medical answers', () => {
  const mapping = loadServer();
  for (const language of ['pt', 'pt-BR', 'pt_BR', 'Portuguese', 'Português', ' Português (Brasil) ']) {
    const submission = { preferred_language: language, session_id: 'test-session',
      circumcised: 'Yes', tobacco_use: 'No', body_type: 'athletic',
      prior_procedure_list: ['Fat transfer'], outcome: 'NOT_ELIGIBLE' };
    for (const map of Object.values(mapping)) {
      assert.equal(map(submission, null).Preferred_Language, 'Portuguese');
    }
    const complete = mapping.mapCompleteBase(submission, null);
    assert.equal(complete.Circumcised, 'Yes');
    assert.equal(complete.Tobacco, 'No');
    assert.equal(complete.Body_Type, 'athletic');
    assert.equal(complete.Previous_Penis_Surgeries[0], 'Fat transfer');
    assert.equal(complete.Outcome, 'NOT_ELIGIBLE');
  }
  for (const [input, expected] of [['en', 'English'], ['es', 'Spanish'], ['ar', 'Arabic']]) {
    assert.equal(mapping.mapLeadBase({ preferred_language: input }, null).Preferred_Language, expected);
  }
});

function loadBooking(lead, owner = {}, userFails = false) {
  let route;
  const app = { get(_path, handler) { route = handler; } };
  const express = { application: { listen() {} } };
  const requests = [];
  const fetch = async (url) => {
    requests.push(url);
    if (url.includes('/oauth/v2/token')) return { ok: true, json: async () => ({ access_token: 'test', expires_in: 3600 }) };
    if (url.includes('/users/')) return { ok: !userFails, status: userFails ? 403 : 200, json: async () => userFails ? { code: 'SCOPE_ERROR' } : { users: [owner] } };
    return { ok: true, json: async () => ({ data: lead ? [lead] : [] }) };
  };
  const context = vm.createContext({ express, fetch, testApp: app, console,
    URLSearchParams, process: { env: { ZOHO_CLIENT_ID: 'test', ZOHO_CLIENT_SECRET: 'test', ZOHO_REFRESH_TOKEN: 'test' } } });
  const code = stripImports(read('server-bootstrap.js')).replace('await import("./server.js");', 'capturedApp = testApp; capturedListenArgs = [];');
  vm.runInContext(code, context);
  return async (lang) => {
    let body, http = 200;
    const res = { status(s) { http = s; return this; }, json(b) { body = b; return this; } };
    await route({ query: { session_id: 'test-session', lang } }, res);
    return { body, http, requests };
  };
}

const stableLead = { id: 'lead-test', Created_Time: '2020-01-01T00:00:00Z', Owner: { id: 'owner-test' }, Created_By: { id: 'creator-test' },
  Lead_Owner_Calendar_Link: 'https://calendly.com/test/english', Spanish_Calendar_Link: 'https://calendly.com/test/spanish', Arabic_Calendar_Link: 'https://calendly.com/test/arabic' };

test('Portuguese booking retains locale and uses verified Lead English field first', async () => {
  for (const lang of ['pt', 'pt-BR', 'Portuguese']) {
    const { body, requests } = await loadBooking(stableLead)(lang);
    assert.equal(body.status, 'ready');
    assert.equal(body.language, 'pt');
    assert.equal(body.calendarLanguage, 'en');
    assert.equal(body.bookingUrl, stableLead.Lead_Owner_Calendar_Link);
    assert.equal(body.calendarField, 'Lead_Owner_Calendar_Link');
    assert.equal(requests.some(url => url.includes('/users/')), false);
  }
});

test('existing language booking fields stay unchanged', async () => {
  for (const [lang, field] of [['en', 'Lead_Owner_Calendar_Link'], ['es', 'Spanish_Calendar_Link'], ['ar', 'Arabic_Calendar_Link']]) {
    const { body } = await loadBooking(stableLead)(lang);
    assert.equal(body.language, lang);
    assert.equal(body.calendarField, field);
    assert.equal(body.bookingUrl, stableLead[field]);
  }
});

test('Portuguese booking uses owner profile secondary and handles missing calendar', async () => {
  const lead = { ...stableLead, Lead_Owner_Calendar_Link: null };
  const { body } = await loadBooking(lead, { Calendar_Link: 'https://calendly.com/test/owner' })('pt');
  assert.equal(body.status, 'ready');
  assert.equal(body.source, 'zoho_user_profile');
  assert.equal(body.calendarField, 'Calendar_Link');
  const missing = await loadBooking(lead, {}, true)('pt');
  assert.equal(missing.body.status, 'calendar_missing');
  assert.equal(missing.body.language, 'pt');
});

test('assignment and calendar propagation remain pending for recent Portuguese leads', async () => {
  const recent = { ...stableLead, Created_Time: new Date().toISOString(), Lead_Owner_Calendar_Link: null };
  const assigning = await loadBooking({ ...recent, Owner: recent.Created_By })('pt');
  assert.equal(assigning.body.status, 'pending');
  assert.equal(assigning.body.reason, 'owner_assignment_in_progress');
  const syncing = await loadBooking(recent)('pt');
  assert.equal(syncing.body.reason, 'calendar_sync_in_progress');
  const missing = await loadBooking(null)('pt');
  assert.equal(missing.body.reason, 'crm_record_not_ready');
});
