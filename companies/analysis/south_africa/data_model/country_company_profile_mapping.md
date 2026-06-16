# South Africa Company Profile — Source Mapping

> **Paid registry, open procurement.** The authoritative key is the **CIPC
> registration number** (`YYYY/NNNNNN/NN`), but CIPC is **paid** and the number is
> **not in the open data**. The open layer is **eTenders OCDS** (public domain):
> company **names** + ZAR award values + government buyers — partial, **name-keyed**
> (no registration number). **No open join key**; cross-source links are name-based.
> SA has VAT (10-digit, starts 4) + SARS income-tax number, neither openly
> published. Private financials paid (CIPC AFS); listed via JSE/SENS. Directors are
> personal data (POPIA).

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| identity.legal_name | etenders_ocds | awards[].suppliers[].name | name | ongoing | open (PDDL) | Open name; or CIPC enterprise_name (paid). |
| identity.registration_number | cipc_registry | company.registration_number | reg_no | live | paid | Authoritative id; not in OCDS. |
| identity.csd_number | csd_suppliers | supplier.csd_number | csd_no | live | gated | Links CIPC+SARS. |
| tax_identifiers.income_tax_number / vat_id | — | — | — | — | not available | SARS; not openly published. |
| legal_identity.company_status / type / registration_date / registered_address | cipc_registry | company.* | reg_no | live | paid | PLANNING-ONLY. |
| procurement[] | etenders_ocds | awards[].value / buyer.name / tender.title | name | ongoing | open (PDDL) | Award value = contract value, NOT revenue. |
| bee_status | csd_suppliers | supplier.bee_status | csd_no/reg_no | live | gated | SA-specific (B-BBEE). |
| officers[] | cipc_registry | company.directors | reg_no | live | paid | PLANNING-ONLY; personal data (POPIA). |
| financial_statements[] | cipc_registry / jse_sens_listed | company.AFS / results.* | reg_no / share_code | filing/periodic | paid / exchange terms | PLANNING-ONLY; ZAR. |

## Source precedence

1. **etenders_ocds** — the open layer: company names + procurement activity. Primary
   open source (partial coverage; government suppliers).
2. **cipc_registry** — authoritative identity (registration number, status, type,
   directors, AFS); paid, planning-only.
3. **csd_suppliers** — links CIPC + SARS + B-BBEE; gated.
4. **jse_sens_listed** — listed-company financials.

Conflict rules:
- **Identity**: OCDS gives a name; CIPC (paid) is authoritative for the registration
  number, status, type, directors.
- **Financials**: JSE/SENS (open) for listed; CIPC AFS (paid) for private; OCDS
  award value is **not** company revenue.

## Join keys

- **No open join key.** The authoritative key is the **CIPC registration number**
  (paid, not in OCDS). The CSD carries the registration number but is gated. The
  open OCDS keys suppliers on **name**, so cross-source joins are **name-based and
  approximate**.

## Missing / restricted data

- **The full company register** (CIPC) — paid.
- **Registration / tax / VAT numbers** in the open layer — absent (names only).
- **Private-company financials** — paid (CIPC AFS); listed via JSE/SENS.
- **Directors** — paid CIPC; personal data (POPIA).
