# SCB Företagsregistret / FDB free API — Field Catalog

## Source Summary

- Country: Sweden
- Source type: statistical_business_register_api
- Organization: Statistics Sweden (Statistiska centralbyrån, SCB)
- URL: https://www.scb.se/vara-tjanster/bestall-data-och-statistik/foretagsregistret/foretagsregistrets-tjanster/foretagsregistrets-webbtjanster/
- License: **CC0 1.0** (public-domain dedication; no attribution required)
- Access: public, **client-certificate** auth today (request via scbforetag@scb.se); **API-key model from September 2026**
- Freshness: nightly Mon–Fri import; most variables weekly, some annual; sourced mainly from Skatteverket
- Record shape: REST JSON/XML; company (företag) + workplace (arbetsställe) records; one company → 0..n workplaces. **Max 2,000 rows/request; 10 requests / 10 seconds.** ~1.80M companies, ~1.44M local units.
- Primary keys: organisationsnummer (company), CFAR-nummer (workplace)
- Join keys: organisationsnummer

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| foretag.organisationsnummer | organisationsnummer / personnummer | Company id (join key) | string | identifier | — | personnummer for sole traders |
| foretag.foretagsnamn | företagsnamn | Company name | string | legal_name | — | Bolagsverket is name authority |
| foretag.juridisk_form | juridisk form | Legal form (coded) | string | legal_form | — | Cross-walk to Bolagsverket |
| foretag.adress | adress | Postal/visiting address | object | address | — | — |
| foretag.postnummer | postnummer | Postal code | string | address | — | — |
| foretag.postort | postort | Postal town | string | address | — | — |
| foretag.kommun | kommun | Municipality | string | geography | — | Authoritative kommun source |
| foretag.lan | län | County | string | geography | Stockholms län | Authoritative län source |
| foretag.sni_kod | SNI-kod | Industry code (SNI 2025) | string | activity | — | Fuller SNI than Bolagsverket; repeatable |
| foretag.storleksklass_anstallda | storleksklass anställda | Employee size-class band | string | employment | — | Band only, never exact headcount |
| foretag.moms_flagga | moms (VAT) flag | VAT-registered | boolean | status | — | Confirms a derived VAT number is active |
| foretag.arbetsgivare_flagga | arbetsgivare flag | Employer-registered | boolean | status | — | — |
| foretag.fskatt_flagga | F-skatt flag | F-tax approval | boolean | status | — | Strong "actively trading" signal |
| arbetsstalle.cfar_nummer | CFAR-nummer | 8-digit workplace id | string | identifier | — | SCB-only; local_units[] primary key |
| arbetsstalle.namn_adress | arbetsställe namn + adress | Workplace name/address/geo | object | address | — | Per-site |
| arbetsstalle.sni_kod | SNI-kod (arbetsställe) | Workplace industry code | string | activity | — | Can differ from company SNI |
| arbetsstalle.huvud_del | huvud-/del-arbetsställe | Main vs subsidiary site | string | relationship | huvudarbetsställe | Identify principal site |

## Interpretation Notes

- **No record was pulled** (certificate auth not obtained). Fields are taken from the SCB
  *postbeskrivning* field-documentation PDFs (`postbeskrivning-foretag.pdf`,
  `postbeskrivning-arbetsstalle.pdf`, `variabelbeskrivning-api-sni-2025.pdf`) as summarized in the
  investigation. **Confirm exact JSON keys against the postbeskrivning + a real authenticated
  response.**
- **Role: the universe seed.** SCB is the best source for the *full* orgnr list, **workplaces
  (arbetsställen) with CFAR ids**, **municipality/county geography**, **SNI** (company + workplace
  level), **employee size-class**, and the **VAT / employer / F-tax register flags**. Use it to
  enumerate orgnr and then enrich each via Bolagsverket.
- **No financial statements.** SCB carries none — financials come only from Bolagsverket annual
  reports.
- **Employment is banded.** `storleksklass anställda` is a size-class (e.g. 0, 1–4, 5–9 …), never an
  exact headcount. Never present it as a precise number.
- **Workplaces.** One company has many CFAR workplaces; model them as a repeatable `local_units[]`
  array keyed by CFAR, each with its own address, SNI, and main/subsidiary flag.
- **Auth migration.** Certificate today → API key in **September 2026** (with pagination + larger
  pages). Build the client to swap auth modes; data license stays CC0.

No `sample_record.json` is provided: certificate auth was not obtained, so no SCB record was
retrieved.
