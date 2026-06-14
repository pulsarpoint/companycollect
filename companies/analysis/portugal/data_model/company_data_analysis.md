# Company Data Analysis For Portugal

## Summary

Portugal is a **closed-data** country for company information. Despite a mature
e-government stack (eportugal.gov.pt, dados.justica.gov.pt), **no open
per-company register exists**:

- The **Registo Comercial** (IRN / Ministério da Justiça) is **paid** per
  *certidão permanente* — there is no free bulk or open API of company records.
- The **publicações obrigatórias** at `publicacoes.mj.pt` (acts companies must
  publish) are behind a **reCAPTCHA** and are not bulk-downloadable.
- The **IES** (Informação Empresarial Simplificada — the annual
  accounting/tax/statistical filing) is **not openly published**.
- The **RCBE** (beneficial-ownership register) is **restricted** (no general
  public access after CJEU C-37/20).
- `dados.justica.gov.pt` is genuinely open (CC-BY-SA) but contains only
  **statistical aggregates** (e.g. counts of online registrations), **not
  per-company rows** — confirmed by downloading `rco.csv` (120-row time series).

What you *can* build openly is an **identity + VAT-validation** shell keyed on the
**NIPC**: the NIPC equals the **NIF** and the **VAT id** is `PT` + NIPC, which
**VIES** validates for free (and sometimes returns a name). Everything richer —
status, activity, capital, sede, officers, shareholders, **financials** — comes
from the **paid register** or a **commercial aggregator** (Racius /
Informa D&B-einforma / Iberinform), with aggregators being the realistic route to
**structured financials** because they pre-parse the IES. The combined profile is
therefore **largely planning-only**.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| registo_comercial | Registo Comercial (IRN/Justiça) | blocked_payment | paid | paid certidão | Authoritative identity/status/officers/owners (planning) |
| ies_financials | IES — Informação Empresarial Simplificada | blocked_authentication | paid | not openly published | Financials (planning) |
| commercial_aggregators | Racius / Informa D&B / Iberinform | planning_only | paid (some free search) | commercial | Realistic identified + structured financials route |
| publicacoes_mj | Publicações obrigatórias (MJ) | blocked_authentication | reCAPTCHA | gov | Mandatory acts (planning) |
| rcbe_register | RCBE beneficial owners | blocked_authentication | restricted | restricted | Beneficial owners (planning) |
| vies_vat | AT / VIES VAT validation | sample_only | public | validation only | Free VAT validation + name signal |
| dados_justica_stats | dados.justica.gov.pt | planning_only | public | CC-BY-SA | Statistics only (context, not companies) |

## What Each Source Contributes

- **registo_comercial** — the authoritative company record (NIPC, firma,
  natureza jurídica, estado, sede, CAE, objeto, capital social, sócios, gerência).
  Paid per certidão; cataloged from public docs only.
- **ies_financials** — balanço (ativo / capital próprio / passivo) +
  demonstração de resultados (volume de negócios / resultado líquido) +
  n.º de trabalhadores, SNC, EUR. Not openly published.
- **commercial_aggregators** — resell the register and **parse the IES** into
  structured multi-year financials; the practical way to get identified data +
  financials at scale. Racius offers a **free basic search** (NIPC/name/CAE/
  capital/status).
- **publicacoes_mj** — mandatory published acts; reCAPTCHA-gated.
- **rcbe_register** — beneficial owners; restricted post-CJEU.
- **vies_vat** — free VAT validation for `PT`+NIPC, occasionally a name — the
  only free way to confirm a company name for a known NIPC.
- **dados_justica_stats** — open statistical time series only (downloaded
  `rco.csv`); useful as market context, not as company records.

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **`registration.nipc`** and groups
fields by real Portuguese concepts: registration, tax_identifiers (free VIES
layer), legal_identity, status, activity, capital, registered_location, officers,
shareholders, beneficial_owners, publications, financial_statements, and a
`registry_statistics` context block. Every paid/restricted/gated section is
annotated `x-access` (paid / restricted / blocked_authentication) and personal
data is flagged `x-personal-data`. The `example.json` is **schematic**: the NIPC
and VAT are populated and validatable, but officers/shareholders/owners are
**redacted** and financials are **empty** because no per-company open record is
lawfully downloadable.

## Join And Precedence Rules

- **NIPC** (9 digits) is the universal join key; `nif = nipc`,
  `vat_id = "PT" + nipc`. A known VAT yields the NIPC for free.
- Precedence: **Registo Comercial** (authoritative) > **commercial aggregators**
  (identity + parsed financials) > **IES** (financials) > **VIES** (validation /
  name) > **RCBE** (owners, restricted) > **publicacoes.mj** (acts, gated).
  `dados.justica` statistics never feed per-company fields.

## Missing Or Restricted Data

- **Everything per-company beyond the NIPC/VAT is paid or planning-only** — no
  open register exists for Portugal.
- **Financials** (IES) are not openly published; obtain via a paying vendor.
- **Beneficial owners** (RCBE) are restricted.
- **Publications** are reCAPTCHA-gated.
- Open data (`dados.justica`) is **statistical only**.

## Common Mapper Notes

For a cross-country mapper, treat Portugal as **paid/aggregator-first**: derive
`vat_id`/`tax_id` from `company_id` (NIPC), validate via VIES, and expect all
other common fields to require paid access. Do **not** map `dados.justica`
statistics to per-company common fields. See
`common_field_mapping_suggestions.md`.
