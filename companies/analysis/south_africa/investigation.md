# South Africa Company Data Investigation

## Conclusion

South Africa is a **paid-registry** country: the authoritative company register
(**CIPC**) is **not open** (paid per-transaction), and there is **no open company
financial source for private companies**. The realistic **open** layer is
**government procurement (OCDS)**, which surfaces company **names** + public-sector
activity, plus **JSE/SENS** for listed-company financials.

- **Open (best):** **National Treasury eTenders — OCDS API** (public domain, PDDL).
  Government tenders/awards with the buyer, supplier **company names**, and award
  values in ZAR. Partial coverage (firms transacting with government); supplier id
  is the legal **name** only (no CIPC registration number).
- **Authoritative (paid):** **CIPC** company register — registration number, name,
  status, type, directors, and annual financial statements (AFS, iXBRL). Sold via
  CIPC eServices / BizPortal (customer code + fees). No open bulk/API.
- **Gated:** **Central Supplier Database (CSD)** — mandatory government-supplier
  database (links CIPC + SARS); login-gated.
- **Financials:** private — not public; listed — **JSE/SENS**.

## What was verified live

- **eTenders OCDS API works**: `…/OCDSReleases?PageNumber=1&PageSize=3&dateFrom=…&dateTo=…`
  → HTTP 200, full OCDS releases. License = **Open Data Commons PDDL (public
  domain)**; publisher = **National Treasury (South Africa)**. Real awarded
  suppliers: **AMESTRA HOLDINGS** (ESKOM, ZAR 7,724,415,731), **BASIL KE YONA
  CONSTRUCTION** (Johannesburg Water, ZAR 66,534,975), **GRASSROOTS HOLDINGS**
  (Sol Plaatje Municipality, ZAR 2,490,000).
- The OCDS supplier party carries only `legalName` (no `identifier` scheme /
  registration number in the sampled releases).
- **CIPC / BizPortal** reachable (HTTP 200); BizPortal is mainly a company-
  registration service, not a free search. **JSE** reachable (HTTP 200).
- **data.gov.za** unreachable (HTTP 000) in this run.

## Identifiers

- **Company registration number** (CIPC) — format `YYYY/NNNNNN/NN`
  (`2015/123456/07`): year of registration + 6-digit serial + 2-digit type suffix
  (`/07` private company (Pty) Ltd, `/06` public, `/08` non-profit (NPC), `/23`
  external company, `/24` co-operative, etc.). The authoritative id — but in the
  **paid** registry.
- **Income tax number** (SARS, 10-digit) and **VAT number** (10-digit starting with
  `4`) — South Africa has **VAT**, but both are **not openly published**.
- The open **OCDS** data keys suppliers on **name** only (no registration number),
  so it cannot be cleanly joined to CIPC without name matching.

## OCDS schema (verified)

Release: `ocid`, `id`, `date`, `tag` (planning/tender/award/contract/compiled),
`tender` (title, value, procurementMethod, …), `planning`, `parties[]` (each with
`name`, `roles` e.g. supplier/buyer/procuringEntity, `identifier` = `{legalName}`),
`buyer` (`name`), `awards[]` (`suppliers[].name`, `value.amount`,
`value.currency` ZAR, `date`), `contracts[]`. Currency **ZAR**.

## What is NOT openly available

- **The full company register** (CIPC) — paid.
- **Private-company financial statements** — paid (CIPC AFS/iXBRL); listed via JSE.
- **Registration numbers / tax / VAT numbers** in the open OCDS data — names only.
- **Directors** — CIPC (paid); personal data (POPIA).

## Recommended ingestion

1. **eTenders OCDS** API (open, PDDL) — company names + procurement activity
   (buyers, awards, ZAR values), paginated by date.
2. **JSE/SENS** — listed-company financials (join by name / registration number).
3. Treat **CIPC** (registry, directors, AFS) and **CSD** as paid/gated
   authoritative sources.
4. Procurement award/supplier data is public; some supplier names may be sole
   proprietors (personal names) — handle per POPIA.
