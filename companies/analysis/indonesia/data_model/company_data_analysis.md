# Company Data Analysis For Indonesia

## Summary

Indonesian company identity is **split across two official systems**: **AHU Online**
(Ditjen AHU, Ministry of Law) — the **legal-entity registry** for PT/CV/Yayasan,
keyed on the **Nomor SK AHU**, holding NPWP, capital, directors and shareholders
(but **paid via PNBP**, and the host was **geo-blocked** from this environment) —
and **OSS** (BKPM), which issues the **NIB** (13-digit business id) with **KBLI**
activity codes via a public per-company search. The **NIB** is the modern primary
company id; **NPWP** is the tax id and the join key; Indonesia has VAT (**PPN**)
with **no separate VAT number** (PKP status via NPWP).

**Listed-company financials** come from **IDX** (open via browser but
**Cloudflare-gated** for automation); **private-company financials are not open**.
There is **no open bulk register**. The profile is fully **designable**, but
ingestion is constrained by **payment (AHU)**, **per-company JS search (OSS)**, and
**Cloudflare (IDX)** — no per-company values were captured (nothing bypassed).

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| ahu_legal_entity | AHU — Legal Entity Registry | blocked_payment | paid (PNBP); geo-blocked here | paid | Legal-entity identity |
| oss_nib | OSS — Online Single Submission (NIB) | insufficient_transport_info | public per-company search | not stated | NIB + activity |
| idx_listed_financials | IDX — listed-company financials | blocked_authentication | browser; Cloudflare-gated | public disclosure | Listed financials |

(Satu Data Indonesia is recorded in discovery as a statistics-only secondary source,
not modelled here.)

## What Each Source Contributes

- **ahu_legal_entity** — the legal identity: Nama PT, Nomor SK, NPWP, legal form,
  capital, directors, shareholders. Paid; documented from public docs (geo-blocked).
- **oss_nib** — the **NIB** (modern business id), **KBLI** activities, business
  scale (UMK/Non-UMK), status. Public per-company JS search.
- **idx_listed_financials** — listed-company financial statements + annual reports
  (IDR), keyed on the IDX ticker; filings carry NPWP to join back. Cloudflare-gated.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **NIB** with sections: `registration`
(nib/nomor_sk_ahu), `tax_identifiers` (npwp; no separate VAT), `legal_identity`,
`status`, `activity` (KBLI/scale), `registered_location`, `capital` (IDR, paid),
`owners`/`officers` (redacted, paid), `listing` + `financial_statements[]` (IDX,
listed), and `source_provenance[]`. The example uses a real public-knowledge listed
company (PT Bank Central Asia Tbk / BBCA) with identifiers null.

## Join And Precedence Rules

- **NIB** is the modern key; **NPWP** joins AHU ↔ OSS ↔ tax; **ticker** keys the
  listed entity.
- **AHU** authoritative for legal entity; **OSS** for NIB/activity; **IDX** for
  listed financials.

## Missing Or Restricted Data

- **No open bulk register** (AHU paid + geo-blocked; OSS per-company JS).
- **Private financials not open** — only IDX (listed, Cloudflare-gated).
- **No separate VAT number** (PPN; PKP via NPWP).
- **Incorporation/dissolution dates, capital, owners/officers** are paid AHU fields.
- **Directors/shareholders** redacted as personal data.

## Common Mapper Notes

`company_id` = NIB; `registration_number` = Nomor SK AHU; `tax_id` = NPWP (join
key); no `vat_id`. Financials open only for listed companies. Currency **IDR**. See
`common_field_mapping_suggestions.md`.
