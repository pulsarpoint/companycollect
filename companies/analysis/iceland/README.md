# Company data sources for Iceland

## Status

- Official bulk data: **not found (open)** — the company-register bulk/extracts are paid (a fee schedule / gjaldskrá applies)
- Official API: **not found (open)** — per-company web search only
- Open data portal: opingogn.is now redirects to island.is; it does not host the register openly
- License: **restricted** for bulk/certificates; the free per-company overview is public
- Recommended ingestion path: **per-company lookup** (free overview) — no open bulk; bulk + financials are paid

## Best source

**Fyrirtækjaskrá** — the national company register run by **Skatturinn** (Iceland
Revenue and Customs). Every entity is keyed by its **kennitala** (10-digit national
identifier, used for both legal entities and individuals; for companies it is also
the tax id). The **free per-company overview** (`gjaldfrjálst yfirlit`) at
`skatturinn.is/fyrirtaekjaskra/leit/kennitala/{kennitala}` exposes: legal name,
kennitala, registered address (lögheimili), municipality (sveitarfélag), legal
form (rekstrarform), ÍSAT economic activity, VAT-register (VSK) status, and the
responsible person / chair (forráðamaður — personal data).

Verified live: fetched real records — e.g. kennitala `6204830369` **JBT Marel ehf.**
and `4612023490` **Icelandair ehf.** (legal form `E1 Einkahlutafélag (ehf)`,
addresses in Garðabær / Hafnarfjörður).

> **No open bulk/API.** Bulk register extracts and certified certificates
> (Staðfest vottorð) are **paid** (a gjaldskrá / fee schedule applies). The open
> route is per-company lookup only.

## Financial data

Companies file annual accounts electronically with the **Ársreikningaskrá** (Annual
Accounts Register, Skatturinn) for **public disclosure** ("til opinberrar
birtingar"). However, retrieval of the filed accounts is **paid per-document** —
there is no open bulk / XBRL download. Financial data therefore exists but is not
openly available in bulk.

## Identifiers & tax

- **kennitala** — 10-digit national id; the company id **and** the tax id. Company
  kennitalas have +40 added to the day field (so the first two digits are 41–71).
- **VSK-númer** (VAT) — Iceland has **VAT (VSK)**; the VAT registration number is
  **separate** from the kennitala (the register shows VSK status). So a VAT id
  exists but is a distinct registration.

## Next action

Use the free Fyrirtækjaskrá per-company overview (keyed on kennitala) for identity;
treat bulk extracts, certified certificates, and the Annual Accounts Register
(financials) as paid. Sample uses real register data (chairman redacted, POPIA/GDPR).
