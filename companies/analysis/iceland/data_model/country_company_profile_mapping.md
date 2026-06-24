# Iceland Company Profile — Source Mapping

> Keyed on the **kennitala** (10-digit) = company id = tax id. The **Fyrirtækjaskrá**
> gives a **free per-company overview** (no open bulk; paid extracts/certificates).
> Iceland has **VAT (VSK)** — a separate registration from the kennitala. Financials
> (**Ársreikningaskrá**) are filed for public disclosure but **paid** per-document.
> The board chair is personal data (GDPR).

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.kennitala | skatturinn_fyrirtaekjaskra | kennitala | kennitala | live | free overview / paid bulk | Authoritative id + tax id. |
| tax_identifiers.tax_id | skatturinn_fyrirtaekjaskra | kennitala | — | live | free | = kennitala. |
| tax_identifiers.vat_id | skatturinn_fyrirtaekjaskra | vsk_status | — | live | free (status) | VSK number is a separate registration. |
| legal_identity.legal_name | skatturinn_fyrirtaekjaskra | nafn | — | live | free | Primary name. |
| legal_identity.legal_form | skatturinn_fyrirtaekjaskra | rekstrarform | — | live | free | E1 ehf / hf / sf / svf / ... |
| status.status | skatturinn_fyrirtaekjaskra | (presence) | — | live | free | registered / afskráð. |
| activity.isat_code | skatturinn_fyrirtaekjaskra | isat_atvinnugrein | — | live | free | ÍSAT (NACE-based). |
| registered_location.* | skatturinn_fyrirtaekjaskra | logheimili / sveitarfelag | — | live | free | Address + municipality. |
| officers[] | skatturinn_fyrirtaekjaskra | forradamadur | kennitala | live | free chair / paid full board | PERSONAL DATA (GDPR) — redact. |
| financial_statements[] | arsreikningaskra | account.* | kennitala | annual | paid | PLANNING-ONLY; ISK. |

## Source precedence

1. **skatturinn_fyrirtaekjaskra** — authoritative open identity (free per-company
   overview). Primary source.
2. **arsreikningaskra** — authoritative financials (paid; planning-only).

Both are Skatturinn registers joined on the kennitala — no conflicting sources.

## Join keys

- **kennitala (10-digit)** across both registers. It is also the tax id. The **VSK
  (VAT)** number is a separate registration (only the status is open).

## Missing / restricted data

- **Open bulk register / enumeration** — none; per-company lookup only (bulk paid).
- **Financial statements** — paid per-document (Ársreikningaskrá).
- **Certified certificates** (Staðfest vottorð) and **full board** — paid.
- **Personal data** — board chair (forráðamaður), GDPR; redact.
