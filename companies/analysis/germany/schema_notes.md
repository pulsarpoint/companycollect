# Germany — Schema Notes

Source: OffeneRegister.de `de_companies_ocdata.jsonl.bz2` (OpenCorporates company schema).
Observed from live sample (`raw/samples/offeneregister_sample_20.jsonl`).

## Top-level fields observed
```
all_attributes            object  - German-specific register metadata (see below)
company_number            string  - synthetic OpenCorporates id, e.g. "K1101R_HRB150148"
current_status            string  - e.g. "currently registered"
jurisdiction_code         string  - always "de"
name                      string  - legal company name incl. legal form suffix
officers                  array    - directors/representatives (see below)
previous_names            array    - [{ "company_name": "..." }]
subsequent_registrations  array    - links to later/renamed registrations
registered_address        string  - free-text postal address (not parsed)
retrieved_at              string  - ISO8601 timestamp of scrape (mostly 2017-2019)
```

### all_attributes
```
_registerArt              string  - register type: HRB, HRA, GnR, VR, PR
_registerNummer           string  - court-scoped register number, e.g. "150148"
native_company_number     string  - human form, e.g. "Hamburg HRB 150148"
federal_state             string  - Bundesland (English), e.g. "North Rhine-Westphalia"
registered_office         string  - seat / Sitz, e.g. "Düsseldorf"
registrar                 string  - registering court (Amtsgericht)
former_registrar          string  - previous court (optional)
additional_data           object  - booleans for available documents:
                                    AD (current printout), CD (chronological),
                                    HD (historical), DK (document folder),
                                    SI (structured content/XML), UT (docs),
                                    VÖ (publications)
```

### officers[]
```
name              string  - full name
position          string  - e.g. "Geschäftsführer", "Prokurist", "Vorstand"
type              string  - "person" (occasionally company)
start_date        string  - ISO date (optional)
end_date          string  - ISO date (optional; presence ~ dismissed/ended)
other_attributes  object  - firstname, lastname, city, flag (Vertretungsregelung),
                            dismissed (bool), reference_no
```

## Notes / gotchas
- **No tax_id / VAT-ID** in this dataset. VAT (USt-IdNr) would need separate enrichment
  (e.g. EU VIES validation — validation only, not bulk listing).
- **No incorporation/dissolution dates** as clean top-level fields; status is textual.
- `registered_address` is **unparsed free text** (sometimes just a street, sometimes full address
  with postal code + city). Needs address parsing/normalization.
- `_registerNummer` is **not globally unique** — it is scoped per court. Use `company_number`
  (synthetic) or `registrar` + `_registerArt` + `_registerNummer` as the natural key.
- Register types: HRB (Kapitalgesellschaften/GmbH/AG), HRA (Personengesellschaften/sole traders),
  GnR (cooperatives), VR (associations), PR (partnerships).

## Mapping to internal company model
```
company_id          <- company_number
registration_number <- all_attributes._registerNummer
register_type       <- all_attributes._registerArt
tax_id              <- (null; enrich later)
vat_id              <- (null; enrich via VIES)
legal_name          <- name
normalized_name     <- lower(trim(name))
company_type        <- derive from legal-form suffix in name / _registerArt
status              <- current_status
incorporation_date  <- (null; not reliably present)
dissolution_date    <- (null; infer from status text)
registered_address  <- registered_address (parse later)
municipality        <- all_attributes.registered_office
region              <- all_attributes.federal_state
registrar           <- all_attributes.registrar
country             <- "Germany"
source_url          <- https://daten.offeneregister.de/de_companies_ocdata.jsonl.bz2
source_name         <- "OffeneRegister.de"
source_retrieved_at <- retrieved_at
raw_record          <- full JSON line
```

Officers should go to a separate `company_officers` table keyed by `company_id`.

See `normalized/companies.sample.jsonl` and `normalized/companies.sample.csv` for the applied mapping.
