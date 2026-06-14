# Austria Company Profile — Source Mapping

How each section of `country_company_profile.schema.json` is populated. **Austria's defining trait: the
authoritative spine + financials are PAID** (planning-only); the open layer is a **subset** with a **weak
join** (GISA uses a separate id). Company key = **Firmenbuchnummer**; **UID** bridges to VAT.

## Identity / legal / activity / location

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| registration.firmenbuchnummer | firmenbuch / commercial_aggregators | Firmenbuchnummer | **PK (companies)** | continuous | **paid** / ID-gated | with check letter |
| registration.uid | firmenbuch | UID | bridge | continuous | **paid** | ATU######## |
| registration.gisa_zahl | gisa_open_data | GISA-Zahl | **open id** | regular | **open** | **separate id; no guaranteed FN link** |
| legal_identity.name | firmenbuch (else GISA) | Firmenwortlaut / name | — | continuous | paid / open | open via GISA name |
| legal_identity.rechtsform | firmenbuch | Rechtsform | — | continuous | **paid** | GmbH/AG/OG/KG/eU |
| status.firmenbuch_status | firmenbuch | aufrecht/gelöscht | — | continuous | **paid** | + insolvency |
| activity.gewerbeschluessel | gisa_open_data | Gewerbeschlüssel | — | regular | **open** | **open activity proxy** |
| activity.gewerbewortlaut | gisa_open_data | Gewerbewortlaut | — | regular | **open** | licensed-activity text |
| activity.geschaeftszweig | firmenbuch | Geschäftszweig | — | continuous | paid | **free text, no ÖNACE** |
| registered_location.* | firmenbuch (else GISA) | Geschäftsanschrift / Standort | — | continuous | paid / open | |
| capital.stammkapital | firmenbuch | Stammkapital | — | continuous | **paid** | register capital, EUR |
| officers[] | firmenbuch / aggregators | Organe | firmenbuchnummer | continuous | **paid** · PII | GDPR |
| insolvency_events[] | ediktsdatei | edicts | FN / debtor name | daily | free web / IWG feed | distressed signal |

## Financial statements (paid)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| financial_statements[] | firmenbuch_jahresabschluss | Bilanz + GuV | firmenbuchnummer | annual | **paid per doc** | UGB; PDF/filing |
| financial_statements[] (scale) | commercial_aggregators | financials[] | firmenbuchnummer | daily | **paid** | pre-parsed JSON, multi-year |

### Financial precedence
1. **commercial_aggregators** (Compass/KSV1870/firmafind) — structured multi-year JSON; best at scale.
2. **firmenbuch_jahresabschluss** — official per-document (paid), needs PDF/filing parsing.
   No open option. revenue/net_income/employees **null** for Kleinst/klein (no GuV). Dedupe on
   `firmenbuchnummer + fiscal_year`; currency EUR.

## Join & precedence summary

- **Company key**: **Firmenbuchnummer** (paid); **UID** bridges to VAT. **GISA-Zahl** is a **separate open
  id** — the open GISA layer does **not** carry the Firmenbuchnummer, so linking the open activity data to
  the (paid) company spine is **fuzzy (name+location)**. This is the central modeling weakness for Austria.
- **Authority**: Firmenbuch authoritative (paid); GISA open for activity/location subset; Ediktsdatei for
  insolvency status; aggregators for master+financials at scale.
- **Freshness**: register continuous; GISA regular/monthly; insolvency daily; financials annual.

## Missing / restricted data

- **Open per-company master & open financials**: none — paid Firmenbuch / aggregator.
- **Coded activity (ÖNACE)**: not in the register (Geschäftszweig free text); only the **open GISA
  Gewerbeschlüssel** as a proxy (and only for trade-licence holders).
- **Beneficial ownership (WiEReG)**: restricted — not modeled.
- **PII**: officers (paid) + insolvency debtors — GDPR.
- **Open→paid join gap**: GISA-Zahl ≠ Firmenbuchnummer (fuzzy name match).
