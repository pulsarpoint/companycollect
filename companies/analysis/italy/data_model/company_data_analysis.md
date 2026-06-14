# Company Data Analysis For Italy

## Summary

Italy has a **clean single join key (Codice Fiscale)** and a strong, systematic register — but the
**authoritative spine and full financials are PAID**, so the *open* profile is only a **subset**. The
authoritative **Registro Imprese** (InfoCamere / Chambers of Commerce) holds identity, ATECO, capital,
administrators, status, and **bilanci in XBRL** — all via **paid** API/Telemaco (no open bulk). For free,
you get a **per-company subset** (innovative startups/PMI with name + CF + ATECO + size **bands**),
**GLEIF LEI** (LEI↔CF bridge + legal form + partial ownership), and **ANAC** procurement suppliers
(CF/PIVA) — plus **aggregate** statistics (InfoCamere/ISTAT) that don't contribute per-company. The
realistic route to a full master + exact financials is a **commercial aggregator** (Cerved / AIDA-Bureau
van Dijk / Atoka), which resell Registro Imprese data with 10-year XBRL financials.

## Sources Analyzed

| Slug | Source name | Status | Access | License | Role in profile |
|---|---|---|---|---|---|
| registro_imprese | Registro Imprese (InfoCamere) | blocked_payment | paid | contractual | **Authoritative spine** (planning-only) |
| registro_imprese_bilanci_xbrl | Bilanci XBRL | blocked_payment | paid | contractual | **Full financials** (planning-only) |
| startup_pmi_innovative | Startup/PMI innovative | sample_only | public | IODL 2.0 / CC-BY | **Open per-company seed** (subset; bands) |
| gleif_lei | GLEIF LEI (IT) | ready | public | CC0 | LEI↔CF bridge + partial ownership |
| infocamere_open_data | InfoCamere regional open data | ready | public | CC-BY 4.0 | **Aggregate** benchmarks (non-contributing) |

Also in `source_inventory.json` (not given full catalogs): ANAC procurement (open supplier CF/PIVA — an
open seed), ISTAT ASIA (aggregate), commercial aggregators (Cerved/AIDA/Atoka — paid financials at scale),
dati.gov.it (discovery).

## What Each Source Contributes

- **registro_imprese (authoritative spine, paid).** Denominazione, **Codice Fiscale** (PK), Partita IVA,
  **numero REA** (province-scoped), forma giuridica, ATECO, capitale sociale, stato attività,
  amministratori (PII), PEC, sede legale. The complete per-company record — but paid, per-company lookup,
  no open bulk.
- **registro_imprese_bilanci_xbrl (financials, paid).** Full balance sheet + income statement in **XBRL**
  (taxonomy 2018-11-04): totale attivo, patrimonio netto, valore della produzione (revenue),
  utile/perdita (net income), debiti, numero medio dipendenti. Paid per document; XBRL is a strength but
  access is gated.
- **startup_pmi_innovative (open seed, subset).** Free weekly per-company list of innovative startups +
  SMEs with name + **CF** + ATECO + region + **revenue/employee BANDS**. The only open per-company
  financial signal — but ranges, and a non-representative subset.
- **gleif_lei (open cross-reference).** LEI ↔ **Codice Fiscale/REA** bridge, legal name, ELF legal form,
  status, and **partial ownership** (Level-2 parent links). CC0; covers LEI holders only.
- **infocamere_open_data (aggregate).** CC-BY counts of active companies by ATECO/territory/time — for
  benchmarks/denominators only; does **not** contribute per-company.

## Proposed Country Company Profile

`country_company_profile.schema.json` (+ schematic `.example.json`) models an Italy-specific object:
`registration` (CF + Partita IVA + REA + LEI), `legal_identity`, `status`, `activity` (ATECO), `registered_
location`, `capital`, `officers[]` (paid/PII), `ownership` (GLEIF L2 partial), `financial_statements[]`
(multi-source: paid exact XBRL vs open bands), and `source_provenance[]`. Each field carries `x-source`
and, where paid, `x-access: paid`; financial entries carry a `source` discriminator.

## Join And Precedence Rules

- **Single key `codice_fiscale`** across sources; reconcile with **Partita IVA** (often equal, not always)
  and **numero REA** (province-scoped); bridge **LEI↔CF** via GLEIF `registeredAs`.
- **Authority**: Registro Imprese authoritative (paid). Open sources are a **seed** (CF + name + ATECO +
  bands + LEI), not a master — enrich authoritatively via the paid register or an aggregator.
- **Financial precedence**: bilanci XBRL / aggregator (exact, paid) → startup bands (open, ranges). Dedupe
  on `CF + fiscal_year + accounts_type`; exact figures nullable for micro/abbreviato filers.
- **Freshness**: register continuous; startup weekly; GLEIF daily; bilanci annual.

## Missing Or Restricted Data

- **Open per-company master**: none (paid register / aggregator).
- **Open exact financials**: none — paid bilanci XBRL; open data gives **bands** only.
- **Beneficial ownership (titolare effettivo)**: regulated access, not openly modeled; GLEIF L2 is partial.
- **PII**: amministratori (paid) — GDPR.
- **Aggregate-only** open data (InfoCamere/ISTAT) doesn't contribute per-company.

## Common Mapper Notes

See `common_field_mapping_suggestions.md`. Italy is **clean-key but mostly paid**: one `codice_fiscale`
joins everything, ATECO exists, but the authoritative spine + exact financials are paid — a cross-country
mapper gets only a subset (startups/PMI + GLEIF + ANAC) for free, with **band** financials. Reconcile
CF/Partita IVA/REA; bridge LEI↔CF; currency EUR.
