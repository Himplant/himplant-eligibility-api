# Brazilian Portuguese support

Implemented 2026-10-20 against production commit a17a5ac597195ae47d8a2acdca7c8e7cd8bbb63c.

- Lead, partial and complete submissions recognize `pt`, `pt-BR`, `pt_BR`, `Portuguese`, `Português`, `Portugues` and `Português (Brasil)` as Zoho `Preferred_Language: Portuguese`.
- Zoho Leads metadata was checked: Portuguese already exists in Preferred_Language. No field or picklist changes were needed.
- Medical answer IDs, eligibility outcomes, lead matching, assignment grace periods, retries and deduplication are unchanged.
- No Portuguese calendar field exists in the inspected Leads, Users or Surgeons metadata. Portuguese owner-booking requests explicitly use the existing English calendar: Lead `Lead_Owner_Calendar_Link` first, then owner `Calendar_Link`. Responses preserve `language: pt` and report `calendarLanguage: en` when ready. Surgeon lookup retains its existing English fallback.
- A dedicated Portuguese calendar requires verified CRM fields and real booking links before changing this mapping. The interface language does not imply a Portuguese-speaking consultation.

Run `npm test` for isolated mapping and booking-route tests; requests are mocked and create no CRM records.
