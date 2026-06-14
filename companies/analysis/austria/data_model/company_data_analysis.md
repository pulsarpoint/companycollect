# Company Data Analysis For Austria

## Summary

Austria is a **paid-register** country (with Germany/Italy): the authoritative **Firmenbuch** and the
**annual accounts (Jahresabschluss)** are accessed mostly **for a fee** via clearing houses
(Verrechnungsstellen), with **no open bulk company master and no open bulk financials**. The *open* layer
is a **subset**: **GISA "Gewerbe in Österreich"** (active trade authorizations, no personal data, on
data.gv.at) and the free **insolvency gazette** (Ediktsdatei). The catch is that the open GISA data uses a
**separate id (GISA-Zahl)** with **no guaranteed Firmenbuchnummer link**, so the open activity/location
layer only **fuzzy-links** (name+location) to the paid company spine. For a full master + structured
financials at scale, a **commercial aggregator** (Compass / KSV1870 / firmafind) is the realistic route.

## Sources Analyzed

| Slug | Source name | Status | Access | License | Role in profile |
|---|---|---|---|---|---|
| firmenbuch | Firmenbuch (Companies Register) | blocked_payment | paid / ID-gated | contractual | **Authoritative spine** (planning-only) |
| firmenbuch_jahresabschluss | Jahresabschluss (annual accounts) | blocked_payment | paid per doc | contractual | **Financials** (planning-only) |
| gisa_open_data | GISA — Gewerbe in Österreich | insufficient_transport_info | public | open (CC-BY?) | **Open activity/location subset** |
| ediktsdatei | Ediktsdatei / Insolvenzdatei | blocked_license | free web / IWG feed | free web; feed licensed | Insolvency/status signal |
| commercial_aggregators | Compass/KSV1870/firmafind/D&B | blocked_payment | paid | commercial | Master + financials at scale |

Also in `source_inventory.json`: WiEReG (beneficial ownership, restricted — excluded), data.gv.at (catalog),
Statistik Austria (aggregate).

## What Each Source Contributes

- **firmenbuch (authoritative spine, paid).** Firmenbuchnummer, Firmenwortlaut, Rechtsform, UID,
  Sitz/Geschäftsanschrift, Stammkapital, Geschäftszweig (free text), Organe (PII), Status, Eintragungsdatum.
  Free brief extract + an ID-Austria-gated API; full data is paid via clearing houses.
- **firmenbuch_jahresabschluss (financials, paid).** UGB annual accounts (Bilanzsumme, Eigenkapital,
  Umsatzerlöse, Jahresergebnis) from the Urkundensammlung, per document for a fee; PDF/structured filing.
  Size classes (§221 UGB) limit small-company disclosure (often no revenue).
- **gisa_open_data (open subset).** Active trade authorizations (GISA-Zahl, name, location, Gewerbewortlaut,
  Gewerbeschlüssel) without personal data — the **open activity/location layer** and the only **open** company-
  adjacent dataset. Separate id; fuzzy link to the spine.
- **ediktsdatei (insolvency signal).** Free per-case web queries of insolvency proceedings; a structured
  JSON feed needs an IWG licence. Flags distressed/insolvent companies (join via FN/debtor name).
- **commercial_aggregators (scale).** Pre-parse Firmenbuch + Jahresabschluss into structured JSON
  (multi-year financials, officers) — the practical route to the full population with financials.

## Proposed Country Company Profile

`country_company_profile.schema.json` (+ schematic `.example.json`) models an Austria-specific object:
`registration` (Firmenbuchnummer + UID paid; GISA-Zahl open), `legal_identity`, `status` (+ insolvency),
`activity` (open GISA Gewerbeschlüssel + paid free-text Geschäftszweig), `registered_location`, `capital`
(paid), `officers[]` (paid/PII), `insolvency_events[]` (Ediktsdatei), `financial_statements[]` (paid,
size-class nullability), and `source_provenance[]`. Every field carries `x-source` and, where paid,
`x-access: paid`; financial entries carry a `source` discriminator.

## Join And Precedence Rules

- **Company key**: **Firmenbuchnummer** (paid); **UID** bridges to VAT. **GISA-Zahl** is a **separate open
  id** with no guaranteed FN link → the open layer **fuzzy-matches** (name+location) to the spine. The
  central modeling weakness.
- **Authority**: Firmenbuch authoritative (paid); GISA for open activity/location; Ediktsdatei for
  insolvency; aggregators for master+financials at scale.
- **Financial precedence**: aggregator (structured, scale) → official per-document (paid). Dedupe on
  `firmenbuchnummer + fiscal_year`; revenue/net_income null for Kleinst/klein filers.
- **Freshness**: register continuous; GISA regular; insolvency daily; financials annual.

## Missing Or Restricted Data

- **Open per-company master & open exact financials**: none — paid Firmenbuch / aggregator.
- **Coded activity (ÖNACE)**: not in the register; only the open **GISA Gewerbeschlüssel** proxy (trade-
  licence holders).
- **Beneficial ownership (WiEReG)**: restricted — not modeled.
- **PII**: officers (paid) + insolvency debtors — GDPR.
- **Open→paid join gap**: GISA-Zahl ≠ Firmenbuchnummer (fuzzy name match).
- **GISA file URL** unresolved in discovery (data.gv.at JS portal) — resolve before implementation.

## Common Mapper Notes

See `common_field_mapping_suggestions.md`. Austria is a **paid-register case**: a cross-country mapper gets
only an open subset (GISA + insolvency) for free; identity/financials/capital/officers are paid. Two id
systems (Firmenbuchnummer vs GISA-Zahl) that don't share a key → open layer is fuzzy-linked. Activity code
only open via GISA; ownership not open; financials paid with small-company nullability; currency EUR.
