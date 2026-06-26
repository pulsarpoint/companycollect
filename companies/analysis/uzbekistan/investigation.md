# Uzbekistan Company Data Investigation

## Goal

Find official/open sources for Uzbek company data: registry, identifiers, status, activity,
director, and listing data, with reproducible access notes.

## What was found

Uzbekistan has a known open company register, but its hosting portal and the tax committee
are **firewalled from this investigation environment**; only the statistics agency and the
stock exchange were reachable.

1. **EGRPO — Unified State Register of Enterprises and Organizations (ЕГРПО)** — the
   **authoritative** Uzbek company register, maintained by the **Statistics Agency**
   (`stat.uz`) and published via the national **open-data portal `data.egov.uz`**. It is
   keyed on the **STIR/INN** (9-digit taxpayer identification number) and the **EGRPO
   statistical code**, with name, legal form, status, registration date, registered address,
   **OKED** activity, and director. **From this environment both `data.egov.uz` and
   `data.gov.uz` are FIREWALLED**: `https://data.egov.uz` timed out / `http://data.egov.uz`
   returned **connection refused**, and `https://data.gov.uz` timed out. So the register was
   **not reachable** here; documented from public knowledge only, nothing captured. Director
   is personal data. (Status `unavailable` — environmental firewall, not a real-world block.)

2. **State Tax Committee (`soliq.uz`)** — provides **taxpayer search by STIR/INN** and a
   VAT-payers registry. From this environment `soliq.uz` **timed out** (firewalled) — not
   reachable. Documented from public knowledge; nothing captured. Identifier = **STIR/INN**.
   Complements EGRPO with tax/VAT status. (Status `unavailable`.)

3. **Republican Stock Exchange 'Toshkent' (`uzse.uz`)** — lists issuers/securities.
   **Reachable**, but `/issuers/` returns a small (~11 KB) **JS SPA shell** — issuer data is
   loaded client-side, and the JSON API path was **not found** (`uzse.uz/api/...` returns
   JSON `{"status":404,...}` for guessed paths, confirming a REST backend exists but the
   correct route wasn't located). Browser-public but not cleanly automatable from here.
   Listed companies only. (Status `useful_secondary_source`.)

4. **Statistics Agency (`stat.uz`)** — **reachable**; the **custodian of the EGRPO**. It
   publishes statistics and **links out to the firewalled `data.egov.uz`**; it also holds an
   ODI-certified business dataset (`theodi.org/datasets/221261`). Useful as the entry point
   to the register, but the per-company register itself is served via the firewalled portal.
   (Status `useful_secondary_source`.)

## Identifiers

- **STIR / INN** — 9-digit taxpayer identification number; the universal Uzbek company key
  (EGRPO + tax committee). (STIR = Uzbek; ИНН = Russian.)
- **EGRPO code** — statistical register code.
- **OKED** — economic-activity classifier (KZ/UZ analogue of NACE/ISIC).
- **ISIN** — for UZSE-listed securities.

## What was NOT found (reachable)

- No reachable open bulk/API for the EGRPO register (data.egov.uz / data.gov.uz firewalled).
- No reachable tax/VAT search (soliq.uz firewalled).
- No clean open API for UZSE listed companies (SPA; API route not found).

## Conclusion

Uzbekistan's authoritative open register (**EGRPO via data.egov.uz**) and tax committee
(**soliq.uz**) are **firewalled from this environment** — a network/geo block, not a
real-world access restriction; from an unblocked network the EGRPO open dataset is the
recommended source (STIR/INN-keyed). **stat.uz** is the reachable custodian/entry point, and
**UZSE** is a reachable browser-public (SPA) listed source. Nothing was bypassed or
fabricated; the firewalled sources are documented from public knowledge only.

## Recommended ingestion approach

From an **unblocked network**: pull the EGRPO open dataset/API on `data.egov.uz` keyed on
**STIR/INN**; use `soliq.uz` for tax/VAT status and `uzse.uz` for listed companies (locate
its SPA data endpoint). Convert dates; map **OKED** activity; redact the director's name
(personal data). Confirm the dataset/API shape and license on data.egov.uz when reachable.
