# Bosnia and Herzegovina — combined profile mapping

## Join keys & precedence

- **Primary join key: JIB** (13-digit unique id = company id = tax id) — shared by
  all registers (RS court, FBiH/Brčko court, APIF RFI / FIA financials, UINO).
- **Entity routing**: RS entities resolve from `rs_business_register`
  (`bizreg.esrpska.com`, JSON, RECOMMENDED); FBiH + Brčko from
  `fbih_brcko_court_register` (`bizreg.pravosudje.ba`, APEX, per-company).
- **Precedence**: the court register that holds the entity is authoritative for
  identity/status/activity/address. Financials come only from APIF RFI (RS) / FIA
  (FBiH), paid, per company. VAT from UINO.

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.jib | rs_business_register | Records[].JIB | JIB | authoritative | also from FBiH/Brčko portal |
| registration.mbs | rs_business_register | Records[].MBS | JIB | authoritative | court reg number |
| registration.mb | rs_business_register | Records[].MB | JIB | authoritative | statistical number |
| registration.entity | derived | RS vs FBiH vs Brčko | JIB | n/a | routes the lookup |
| tax_identifiers.tax_id | rs_business_register | Records[].JIB | JIB | authoritative | = JIB |
| tax_identifiers.pdv_broj | uino_pdv | UINO lookup | JIB | UINO | 12-digit VAT, separate |
| legal_identity.business_name | rs_business_register | Records[].PoslovnoIme | JIB | authoritative | FBiH: Naziv |
| legal_identity.short_name | rs_business_register | Records[].SkracenoPoslovnoIme | JIB | authoritative |  |
| legal_identity.legal_form | derived | from name/extract | JIB | derived | d.o.o./a.d./d.d./s.p. |
| status.status_text | rs_business_register | Records[].StatusPoslovniSubjekatOpis | JIB | authoritative | strip HTML |
| activity.activity_code/text | rs_business_register | Records[].PreteznaDjelatnost | JIB | authoritative | KD BiH ~NACE |
| registered_location.registered_address | rs_business_register | Records[].Sjediste | JIB | authoritative | free-text |
| registered_location.branch_count | rs_business_register | Records[].PoslovneJedinice | JIB | authoritative | branch list endpoint |
| owners[] | rs_business_register | Records[].Osnivaci | JIB | authoritative | REDACT natural persons |
| officers[] | rs_business_register | Records[].OdgovornoLice / representatives | JIB | authoritative | REDACT (personal data) |
| financial_statements[] | apif_rfi_financials | bilans stanja/uspjeha | JIB | planning-only | paid; BAM |

## Freshness

- RS / FBiH / Brčko court registers: **live**.
- APIF RFI / FIA financials: **annual** (paid, per company).
- UINO VAT: **live** (per company).

## Missing-data notes

- **No open bulk** for any source; everything is per-company.
- **No working national open-data portal** (`data.gov.ba` did not resolve).
- **FBiH/Brčko transport unconfirmed** (Oracle APEX) — `insufficient_transport_info`.
- **Financials paid** — `blocked_payment`; values are planning-only.
- **Personal data** (founders/representatives) redacted in committed samples.
