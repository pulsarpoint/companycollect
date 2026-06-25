# Hong Kong Schema Notes

## Identifiers

- **CR Company Number** — issued by the Companies Registry; the registry key in ICRIS.
  (Not present in the open RNC063 feed.)
- **BR Number** — Business Registration number issued by the Inland Revenue Department;
  8-digit; the identifier exposed in the **open** RNC063 feed; de-facto business/tax id.
- **Stock Code** — HKEX listed-security code (listed companies only).

## CR Open Data — RNC063L (newly incorporated LOCAL companies) — observed fields

| Field | Meaning |
|---|---|
| Seq | Row sequence within the weekly file |
| Current Company Name in English | English registered name |
| Current Company Name in Chinese | Chinese registered name (may be empty) |
| BR Number | 8-digit IRD Business Registration number — primary key in this feed |
| Date of Incorporation | Incorporation date (DD-MM-YYYY) |
| Date of Change of name | Name-change date (DD-MM-YYYY) if this row is a name change |

## CR Open Data — RNC063F (newly registered NON-HK companies) — observed fields

| Field | Meaning |
|---|---|
| Seq | Row sequence |
| Current Corporate Name / Other Corporate Name | Corporate name of the non-HK company |
| Current Approved Name for Carrying on Business in H.K. | Approved HK business name (may be empty) |
| BR Number | 8-digit IRD Business Registration number |
| Date of Registration | HK registration date (DD-MM-YYYY) |
| Date of Change of name | Name-change date (DD-MM-YYYY) if applicable |

## ICRIS full register — fields (from public documentation, NOT captured)

CR Company Number, Company Name (English/Chinese), Company Type (e.g. private company
limited by shares), Company Status (e.g. Live / Dissolved), Date of Incorporation,
Registered Office Address, Directors, Company Secretary, Charges, filed documents.
Pay-per-use; directors/secretary are personal data (PDPO) — redact.

## HKEX List of Securities — fields (static xlsx is a template)

Stock Code, Name of Securities, Category, Sub-Category, Board Lot, Par Value, ISIN.
Populated server-side; not captured via the static URL.

## Formats, language, encoding

- Languages: English + Traditional Chinese (bilingual). CSV encoding UTF-8 with BOM.
- Dates: **DD-MM-YYYY** in the CR open feed (Gregorian). Convert to ISO 8601.
- Currency: Hong Kong Dollar (HKD) for any financial fields (not in the open feed).

## Mapping to internal model

- company_id ← BR Number (open feed) / CR Company Number (ICRIS) / Stock Code (listed)
- registration_number ← CR Company Number (ICRIS); BR Number where CR No. unavailable
- tax_id / vat_id ← BR Number (no VAT in HK; BR Number is the business id)
- legal_name ← Current Company Name in English; legal_name_zh ← Chinese name
- status ← derived (newly_incorporated from the feed) / Company Status (ICRIS)
- incorporation_date ← Date of Incorporation / Date of Registration (DD-MM-YYYY → ISO)
- registered_address ← (ICRIS only; not in open feed)
- officers ← directors/secretary (ICRIS only; **redact**)
- source_url, source_name, source_retrieved_at preserved per record
