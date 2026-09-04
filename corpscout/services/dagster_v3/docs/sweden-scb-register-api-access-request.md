# SCB Företagsregistret free API — where and how to request access

Status: research, 2026-09-04, verified against SCB's live pages and the
published layout PDFs on that date. Companion to
`sweden-tax-registration-flags-access-analysis.md`.

## Where to submit

**Email: scbforetag@scb.se** — the single contact point for every
Företagsregistret service (free API, paid extracts, Aviseringar, NÄRA).
There is no web form; access is granted by SCB issuing a certificate and
password (switching to an API key in September 2026).

Reference page (Swedish):
https://www.scb.se/vara-tjanster/bestall-data-och-statistik/foretagsregistret/avgiftsfria-uppgifter-i-foretagsregistret/

## What the request must contain

SCB's page, verbatim: *"Ange följande i din förfrågan för snabbare
hantering:"*

1. Company/organisation name and organisation number (optional but speeds
   handling).
2. Name and e-mail of the person who **accepts the user terms**
   (*användarvillkoren*). Access is conditional on accepting them — get and
   read them in the reply before the certificate is used in production.
3. Name, e-mail and **mobile number** of the person who will **receive the
   certificate and password** (the mobile number is for the credential
   delivery).
4. The **layout(s)** (*postbeskrivning*) wanted — see below.

## Which layouts to ask for

Four layouts exist; two are being discontinued. Ask for the two
single-level ones:

| Layout | Use | Status |
|---|---|---|
| **Företag** | company-level record — carries the tax flags | keep |
| **Arbetsställe** | establishment-level record (address, SWEREF99 coordinates, size class, status) | keep |
| Företag med uppgifter om huvudarbetsstället | combined | *being discontinued* |
| Arbetsställe med uppgifter om företaget | combined | *being discontinued* |

Layout PDFs (same content-asset folder):
`postbeskrivning-foretag.pdf`, `postbeskrivning-arbetsstalle.pdf`, plus
`variabelbeskrivning-api-sni-2025.pdf` (variable definitions, SNI 2025).

### Field groups in the Företag layout

The tax flags live in optional add-on groups (*tilläggsgrupper*); name them
explicitly so the layout SCB configures includes them:

- **TG15Stat_Fskatt** — `Fskattstatus, kod` / text (0 never · 1 registered · 9 deregistered)
- **TG15Stat_Moms** — `Momsstatus, kod` / text (0 · 1 · 3 via representative · 9)
- **TG15Stat_ArbGiv** — `Arbetsgivarstatus, kod` / text (0 · 1 · 2 private · 3 via rep · 4 embassy · 9)
- **TG15Stat_Bol** — `Bolagsstatus` (Bolagsverket status, for cross-checking our register spine)

Worth requesting alongside them (all free, same call):

- base group: `Företagsstatus`, `Stkl` / `Storleksklass` (employee size class)
- **TG07Oms** — turnover size classes (`Stkl, oms`, `Stkl Fin, oms`)
- **TG21AnstSmeJE** — SME employee size class
- **TG18Sektor** — institutional sector
- **TG14Utl** — foreign ownership
- **TG16Andel** — ownership share
- **TG17Firma** — registered company name from Bolagsverket
- **TG11PrivPubl** — private/public
- **TG09Epost_JE**, **TG22Tel_JE** — e-mail and telephone (contact
  candidates; check the user terms on contact-data use before serving)

## Technical facts to plan the ingest around

- REST over HTTPS, JSON or XML responses.
- **Max 2,000 rows per request; 10 requests per 10 seconds per user.**
- No bulk download and no pagination today; SCB announces for
  **September 2026**: *"Auktoriseringen kommer att bytas från nuvarande
  certifikat till API-nyckel. Stöd för paginering införs, antalet
  hemtagningar per anrop ökas och det blir även andra förändringar i
  sökfunktionaliteten."* — expect a client change then.
- Nightly refresh except Saturday night; the three tax flags update weekly.
- Current state only — our weekly observations are the history.
- Change notifications (*Aviseringar om förändringar*, 3,000 SEK setup +
  3,000–100,000 SEK/year) and the new-company subscription remain paid;
  not needed if we sweep weekly.

## Open points to settle in the reply

1. **Redistribution in a commercial product** — the user terms are only
   provided on request; read the clause on *vidareutlämnande /
   kommersiell användning* before the flags ship in a sold product.
2. **Full-register sweeps** — confirm SCB is comfortable with a weekly
   sweep of all ~3.4M companies at the stated rate (≈1,700 requests, ~30
   minutes). If not, restrict to the active-priority tier.
3. **Foreign requester** — the page states no nationality prerequisite;
   the org-number field is optional. State the requesting entity plainly.
4. **Test environment** — none is documented; ask whether one exists.

## Draft request (Swedish)

Subject: **Ansökan om åtkomst till API:et för det allmänna företagsregistret**

> Hej,
>
> Vi önskar få åtkomst till SCB:s avgiftsfria API för det allmänna
> företagsregistret.
>
> **Organisation:** <bolagsnamn>, org.nr <organisationsnummer>
>
> **Person som godkänner användarvillkoren:**
> <namn>, <e-post>
>
> **Person som ska ta emot certifikat och lösenord:**
> <namn>, <e-post>, <mobilnummer>
>
> **Önskade postbeskrivningar:**
> - Företag — inklusive tilläggsgrupperna TG15Stat_Fskatt, TG15Stat_Moms,
>   TG15Stat_ArbGiv, TG15Stat_Bol, TG07Oms, TG21AnstSmeJE, TG18Sektor,
>   TG14Utl, TG16Andel, TG17Firma, TG11PrivPubl, TG09Epost_JE, TG22Tel_JE
> - Arbetsställe
>
> **Avsett ändamål:** uppdatering av vårt företagsregister med aktuella
> registreringsuppgifter (F-skatt, moms, arbetsgivare) och arbetsställen,
> med veckovis hämtning.
>
> Vi ber också att få ta del av användarvillkoren i sin helhet, samt
> besked om villkoren för vidareutlämnande av uppgifter i en kommersiell
> tjänst, och om det finns en testmiljö.
>
> Med vänlig hälsning,
> <namn, titel, telefon>

## After access is granted

1. Store the certificate/password in the server-owned dagster `.env` (never
   in the repo), as with the other source credentials.
2. Write the source design doc from `source-design-doc-template.md`
   (module `sweden_scb_register_api`), then build the weekly sweep into
   `se_company_tax_registration_observations` + `_current`, the
   `is_economically_active` derivation, and establishment ingestion.
3. Record the answer on redistribution terms in that design doc before any
   serving surface exposes the flags.
