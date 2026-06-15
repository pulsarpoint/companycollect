# Vietnam Company Profile — Mapping Report

Vietnam is **portal-gated with no open bulk**: the authoritative NBRP register and
the GDT tax lookup are **CAPTCHA-gated per-company**, full coverage needs a **paid
MOU**, and **financials are open only for listed companies**. Everything keys on
the **enterprise code = tax code** (mã số doanh nghiệp = mã số thuế). The profile
is largely **planning-only**.

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.enterprise_code | nbrp_search | Mã số doanh nghiệp | enterprise code | register | = tax code; gated |
| tax_identifiers.tax_code | gdt_taxpayer_lookup | Mã số thuế | enterprise code | = enterprise code | gated |
| tax_identifiers.vat_id | gdt_taxpayer_lookup | Mã số thuế | enterprise code | derived | no separate VAT no. |
| tax_identifiers.tax_status | gdt_taxpayer_lookup | Tình trạng | enterprise code | GDT | gated |
| legal_identity.legal_name | nbrp_search | Tên doanh nghiệp | — | register | gated |
| legal_identity.legal_form | nbrp_search | Loại hình | — | register | gated |
| status.status_raw/status | nbrp_search | Tình trạng hoạt động | — | register | active/suspended/dissolved |
| activity.vsic_codes | nbrp_search | Ngành nghề kinh doanh | — | register | VSIC; gated |
| incorporation.establishment_date | nbrp_search | Ngày thành lập | — | register | gated |
| registered_location.head_office_address | nbrp_search | Địa chỉ trụ sở | — | register | gated |
| officers[] | nbrp_search | Người đại diện | enterprise code | register | legal rep only; PII |
| financial_statements[] | hose_hnx_ssc_disclosure | balance sheet / income statement | enterprise code (via ticker) | PLANNING-ONLY | listed only; VND |
| (full coverage) | nbrp_bulk_mou | company_master | enterprise code | PLANNING-ONLY | paid MOU |

## Source Precedence

1. **NBRP** (per-company search) — authoritative identity, but **gated**.
2. **GDT** — tax status confirmation, **gated**.
3. **nbrp_bulk_mou** — the only full-coverage route (**paid**) → planning-only.
4. **HOSE/HNX/SSC** — listed-company financials → planning-only.
5. **vn_aggregators** — license-uncertain cross-check.

## Join Keys

- **Enterprise code = tax code** (10–13 digits) is the single universal key —
  register, tax, and (via ticker) listed financials. Vietnam has **no separate VAT
  number**.

## Missing / Restricted

- **All bulk identity** — gated (per-company CAPTCHA) or paid (MOU).
- **Financials** — listed-only; non-listed not published.
- **Shareholders / beneficial owners** — not available openly (only the legal
  representative, which is personal data).
- **No open re-use licence**; aggregators are license-uncertain.
