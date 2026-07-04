# Schema notes

Likely useful fields for a Denmark CVR company record:

- `registration_number`: CVR number
- `legal_name`: current company name
- `previous_names`: historical names when available
- `status`: current status / composed status
- `company_type`: legal form
- `incorporation_date`: start date / valid-from life-cycle date
- `dissolution_date`: end date when present
- `registered_address`: current registered address
- `municipality`: municipality code and name
- `industry_codes`: Danish industry codes
- `production_units`: P-units linked to company
- `contact`: public phone/email/web fields when available
- `employee_intervals`: current annual/monthly employment intervals when available
- `source_url`: CVR detail URL
- `source_retrieved_at`: retrieval timestamp
- `raw_record`: raw source payload or parsed page snapshot

The official API schema uses nested paths under `Vrvirksomhed`, including `Vrvirksomhed.cvrNummer`, `Vrvirksomhed.virksomhedMetadata.nyesteNavn.navn`, `Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse`, `Vrvirksomhed.livsforloeb`, and `Vrvirksomhed.sidstIndlaest`.

`cvrapi.dk` version 6 documentation indicates fields including:

- `vat`
- `name`
- `address`
- `zipcode`
- `city`
- `cityname`
- `protected`
- `phone`
- `email`
- `fax`
- `startdate`
- `enddate`
- `employees`
- `addressco`
- `industrycode`
- `industrydesc`
- `companycode`
- `companydesc`
- `creditstartdate`
- `creditstatus`
- `creditbankrupt`
- `owners`
- `productionunits`

Map `vat` to `registration_number`/`company_id`, `companycode` and `companydesc` to legal form, `industrycode` and `industrydesc` to industry, and preserve `protected` for downstream compliance filtering.
