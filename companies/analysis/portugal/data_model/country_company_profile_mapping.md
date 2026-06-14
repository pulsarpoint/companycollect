# Portugal Company Profile — Mapping Report

This maps the country-specific Portugal company profile to its sources. The
critical fact for Portugal: **there is no open per-company register**. The
Registo Comercial is paid, the IES financials are not openly published,
publicacoes.mj.pt is reCAPTCHA-gated, and dados.justica.gov.pt only exposes
statistical aggregates. The profile is therefore largely **planning-only** — the
NIPC (company id) is derivable and VAT is freely validatable, but identity,
officers, owners, and financials require paid access (register or aggregator).

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.nipc | registo_comercial | nipc | NIPC | register > aggregator > VIES-derived | 9-digit; = NIF; paid |
| tax_identifiers.nif | registo_comercial | nipc | NIPC | = NIPC | legal persons |
| tax_identifiers.vat_id | vies_vat | vatNumber | NIPC | derive PT+NIPC | free |
| tax_identifiers.vat_valid | vies_vat | valid | — | VIES | point-in-time |
| legal_identity.legal_name | registo_comercial | firma | NIPC | register > aggregator > VIES name | paid; VIES may return name |
| legal_identity.legal_form | registo_comercial | natureza_juridica | NIPC | register | Lda./S.A./Unipessoal |
| legal_identity.objeto | registo_comercial | objeto | NIPC | register | paid |
| status.estado | registo_comercial | estado | NIPC | register | paid |
| activity.cae_main | registo_comercial | cae | NIPC | register | CAE Rev.3 |
| capital.capital_social | registo_comercial | capital_social | NIPC | register | EUR |
| registered_location.sede | registo_comercial | sede | NIPC | register | paid |
| officers[] | registo_comercial | gerencia[] | NIPC | register/aggregator | PII; redacted |
| shareholders[] | registo_comercial | socios[] | NIPC | register/aggregator | PII; redacted |
| beneficial_owners[] | rcbe_register | beneficiarios[] | NIPC | RCBE | restricted post-CJEU; PII |
| publications[] | publicacoes_mj | tipo_ato/data_publicacao/texto_ato | NIPC | MJ | reCAPTCHA-gated |
| financial_statements[] | ies_financials | ativo/capital_proprio/passivo/volume_negocios/resultado_liquido | NIPC | aggregator (parses IES) > register | IES not openly published; SNC; EUR |
| registry_statistics | dados_justica_stats | (statistical series) | — | — | NOT per-company; CC-BY-SA |

## Source Precedence

1. **Registo Comercial (IRN/Justiça)** — authoritative for identity, status,
   activity, capital, officers, shareholders, sede. **Paid** per certidão.
2. **Commercial aggregators** (Racius / Informa D&B-einforma / Iberinform) —
   resell register data and **pre-parse the IES** into structured financials.
   The realistic route to **structured financials** and bulk identified data.
   Racius offers a **free basic search** (NIPC/name/CAE/capital/status).
3. **IES** — primary source of financials, but **not openly published** (filed to
   AT/IRN). Use a vendor that parses it.
4. **VIES** — free VAT validation + an occasional **name** for a known NIPC.
5. **RCBE** — beneficial owners; **restricted** (no general public access
   post-CJEU C-37/20).
6. **publicacoes.mj.pt** — mandatory acts; **reCAPTCHA-gated**, not bulk.
7. **dados.justica.gov.pt** — open (CC-BY-SA) but **statistics only**; context.

## Join Keys

- **NIPC** (9 digits) is the universal join key; it equals the **NIF** for legal
  persons, and the **VAT id** is simply `PT` + NIPC. So a known VAT number yields
  the NIPC for free, but the NIPC alone does not unlock the paid register.

## Missing / Restricted

- **All per-company fields** (name, status, activity, capital, sede, officers,
  shareholders, financials) are **paid or planning-only** — no open per-company
  dataset exists for Portugal.
- **Financials** (IES) are not openly published — only via a paying vendor.
- **Beneficial owners** (RCBE) are restricted.
- **Publications** are reCAPTCHA-gated.
- Open data (dados.justica) is **statistical only** — no company rows.
