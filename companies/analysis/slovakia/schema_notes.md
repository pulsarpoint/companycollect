# Slovakia — Schema Notes

## Identifiers

- **IČO** — 8-digit company id; universal join key (RPO `identifiers[].value`,
  RÚZ `ico`).
- **DIČ** — tax id (10-digit); RÚZ `dic`. **IČ DPH** (VAT) = `SK` + DIČ.
- RÚZ internal `id` (accounting-unit id) links to statement/report ids.
- RPO internal entity `id` for `entity/{id}` lookups.

## RPO — entity record (api.statistics.sk/rpo/v1/entity/{id})

| Path | Meaning |
|---|---|
| identifiers[] | IČO with validFrom |
| fullNames[] | name history (value, validFrom/validTo) |
| addresses[] | address history (street, buildingNumber, postalCodes, municipality, country, validFrom/validTo) |
| legalForms[] | legal form `{value, code, codelistCode}` (e.g. 112 = s.r.o.) with validFrom |
| establishment | incorporation date (YYYY-MM-DD) |
| activities[] | business activities (economicActivityDescription, validFrom) |
| statutoryBodies[] | **officers** (stakeholderType e.g. Konateľ, personName, address, validFrom) — PII |
| stakeholders[] | **shareholders** (stakeholderType e.g. Spoločník, personName/orgName, address) — PII |
| equities[] | share capital (valuePaid, currency) |
| deposits[] | per-person capital contributions (personName, amount) — PII |
| otherLegalFacts[] | free-text legal facts/history |
| authorizations[] | signing/representation rules |
| predecessors[] | predecessor entities (identifier, fullName, address) |
| sourceRegister | originating register (e.g. Obchodný register) |
| statisticalCodes | main activity (SK NACE) + actualization date |
| license | inline CC-BY 4.0 text |

Search: `search?identifier={ICO}` → `results[]` (id, identifiers, fullNames,
addresses, establishment, sourceRegister). Then `entity/{id}` for the full record.

## RÚZ — accounting unit (uctovna-jednotka?id=)

| Field | Meaning |
|---|---|
| id | accounting-unit id |
| ico / dic | company id / tax id |
| nazovUJ | name |
| ulica / mesto / psc | street / city / postal code |
| datumZalozenia / datumZrusenia | founded / dissolved date |
| pravnaForma | legal-form code (→ pravne-formy classifier) |
| skNace | SK NACE activity code |
| velkostOrganizacie | organization-size code |
| druhVlastnictva | ownership-type code |
| kraj / okres / sidlo | region / district / settlement codes |
| konsolidovana | consolidated flag |
| idUctovnychZavierok[] | statement ids |
| idVyrocnychSprav[] | annual-report ids |
| zdrojDat | data source (SUSR/FRSR) |
| datumPoslednejUpravy | last change |

## RÚZ — financial statement (uctovna-zavierka?id=)

`obdobieOd`, `obdobieDo` (YYYY-MM), `datumZostaveniaK` (balance date), `typ`
(Riadna/Mimoriadna/…), `idUctovnychVykazov[]` (report ids), `datumPodania`,
`datumSchvalenia`, etc.

## RÚZ — financial report (uctovny-vykaz?id=)

- `obsah.tabulky[]` — each `{nazov:{sk}, data:[...]}`. `data[]` is a **positional**
  array of cell values (strings; empty string = blank). Tables (template 687
  "Úč MUJ"): **Strana aktív** (assets), **Strana pasív** (liabilities/equity),
  **Výkaz ziskov a strát** (income statement).
- `idSablony` — template id; decode `data[]` against `sablona?id=` `riadky[]`
  (`cisloRiadku`, `oznacenie` like A./A.I., `text.sk` label). Each row spans
  several columns (e.g. current/prior period; gross/correction/net for assets).
- `prilohy[]` — PDF attachments (meno, mimeType, digest). Large filers may have
  empty `obsah` + PDF only.

## Classifiers (number→label lookups)

`pravne-formy`, `sk-nace`, `kraje`, `okresy`, `sidla`, `druhy-vlastnictva`,
`velkosti-organizacie`. Cache these to resolve codes.

## Mapping to internal model

| Internal | Slovakia source |
|---|---|
| company_id | IČO |
| registration_number | IČO |
| tax_id | RÚZ dic |
| vat_id | "SK" + dic (if VAT-registered) |
| legal_name | RPO fullNames (current) / RÚZ nazovUJ |
| legal_form | RPO legalForms / RÚZ pravnaForma (decode) |
| status | derive: RÚZ datumZrusenia / RPO otherLegalFacts (no single status flag) |
| incorporation_date | RPO establishment / RÚZ datumZalozenia |
| dissolution_date | RÚZ datumZrusenia |
| registered_address | RPO addresses (current) / RÚZ ulica+mesto+psc |
| activity_code | RÚZ skNace / RPO statisticalCodes |
| financials | RÚZ uctovny-vykaz tables (decoded via sablona), EUR |
| officers | RPO statutoryBodies (PII; redact) |
| owners | RPO stakeholders + deposits (PII; redact) |
| share_capital | RPO equities (EUR) |
