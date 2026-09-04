# Sweden — F-skatt / VAT / employer registration flags: access analysis

Status: research, 2026-09-04. Verified against live source pages and the
published terms documents on that date. Follows up item 1 of
`sweden-new-data-sources-proposal.md`.

## Question

Are the three Skatteverket registration flags per company — approved for
F-skatt, registered for moms (VAT), registered as arbetsgivare (employer) —
obtainable free of charge for a commercial data product, or only under
commercial/restricted terms?

## Answer in one paragraph

**Not from Skatteverket, but yes — free — from SCB.** Skatteverket offers no
usable route: its only API is purpose-restricted to bookkeeping for the
company's own principal/agent, its web e-service is one-company-at-a-time
manual, and its bulk service is for public authorities only. However, SCB's
Företagsregistret carries all three flags as explicit weekly-updated
variables (sourced from Skatteverket), and since 26 June 2025 SCB is legally
prohibited from charging for register data and provides a **free API** with
access on request. That is the route to build on. One gap remains: bulk
download and change feeds are not free — the free API is lookup-style,
2,000 rows per request.

## Route 1 — Skatteverket "Beskattningsengagemang" API: unusable for us

Skatteverket's developer portal publishes exactly one finished API,
*Beskattningsengagemang via API*, whose content is precisely the three
flags. Its general terms ("Bilaga A Allmänna villkor") rule it out:

- **Purpose-locked.** Clause 7.1: *"Användning av API:et får endast ske av,
  eller för, Mottagare av uppgift för ändamålet att inhämta nödvändig
  information för bokföring, redovisning och uppföljning."* and *"API:et får
  inte användas för att hämta uppgifter för något annat ändamål än det ovan
  beskrivna."* Building a company database is not that purpose.
- **Principal/agent model.** The querying party must be *"huvudman eller ha
  befogenhet från huvudmannen att företräda denne"* — i.e. the company
  itself or its registered agent (ombud, registered with Skatteverket).
  Third-party lookups across the register are outside the model.
- **Legal basis is the tax-database statute** (SdbL, lag 2001:181), which
  limits processing to enumerated purposes; the API terms inherit that.
- **Software-company agreement required**, EU/EES hosting, audit logging
  retained 5 years. No fee is stated, but fee is moot given the purpose
  restriction.

Skatteverket's own company-information development page confirms the
posture: they are *investigating* machine access to F-skatt/moms/employer
status for software companies, and state *"Det finns just nu inga planer på
ett API"* for the general lookup service.

## Route 2 — Skatteverket "Hämta företagsinformation" e-service: manual only

The public e-service returns the three flags plus *"uppgift om beslutade
arbetsgivaravgifter de senaste tre månaderna"* (decided employer
contributions, last three months — a useful "has payroll" signal). It is
free, but: *"Du kan söka information om ett företag åt gången"* and *"Du kan
bara göra ett begränsat antal beställningar per tillfälle."* No API, no
automation, screen delivery. The multi-company variant, **E-transport**, is
explicitly *"för statliga och kommunala myndigheter"* — public sector only.

Not a data-pipeline route. Automated scraping of it would violate the
service's intended use and is not recommended.

## Route 3 — SCB Företagsregistret free API: the answer

SCB's register mirrors the Skatteverket flags. The API variable description
(SCB, "Variabelbeskrivning API – SCB:s allmänna företagsregister") defines
them explicitly, each *"finns på företag och uppdateras veckovis"*:

| Variable | Values |
|---|---|
| **F-skattstatus** | 0 never registered · 1 registered · 9 deregistered |
| **Momsstatus** | 0 never · 1 registered · 3 registered via representative · 9 deregistered |
| **Arbetsgivarstatus** | 0 never · 1 regular employer · 2 private employer · 3 via representative · 4 embassy/consulate · 9 deregistered |

SCB also defines the derived concept we want directly: *"I SCB:s
Företagsregister betraktas ett företag som verksamt om det är registrerat
för moms och/eller som arbetsgivare och/eller för F-skatt."* — the exact
"economically active" test proposed in the source doc.

The same API carries **arbetsställen** (establishments, with SWEREF99
coordinates, size class, status, type), employee counts and size classes,
SNI, turnover size classes, A-region, and more — which covers proposal
items 1 *and* 3 (establishments) in one source.

**Cost — free by law.** SCB: the government amended the ordinance on the
general business register effective **26 June 2025** so that *"SCB inte
längre ska ta ut avgifter för uppgifter ur företagsregistret"*, and SCB
therefore *"tillgängliggjort ett avgiftsfritt API som innehåller aktuella
data om företag och arbetsställen."*

**Access mechanics** (from SCB's "Avgiftsfria uppgifter i
företagsregistret"):

- Not anonymous: accept the API terms (*"För att använda API:et behöver du
  godkänna användarvillkoren"*) and request a certificate + password from
  **scbforetag@scb.se**, giving organisation, authorised contact, recipient,
  and desired data layout.
- Limits: **max 2,000 rows per request, 10 requests per 10 seconds**;
  pagination announced for September 2026.
- Nightly refresh (except Saturday night); the tax flags themselves update
  weekly.
- **Current state only** — no history, no delta/change feed in the free
  tier.

**What stays paid:** the *Aviseringstjänst* (continuous change
notifications) and *NÄRA*, per SCB's price list. Full bulk extracts are not
part of the free API.

**Commercial use / redistribution:** the free-API page states no
redistribution restriction; the binding text is the API user terms accepted
at sign-up, which were not retrievable without registering. This must be
read before the flags ship in a sold product (the register is public
statutory data, so a permissive outcome is likely, but it is unverified).

## Eligibility — no EU/Swedish requirement

The free access rests on the EU Open Data Directive's **high-value datasets**
(Regulation 2023/138) and Sweden's amended register ordinance. The
government's press release names the beneficiaries as *företag,
privatpersoner* and *statliga och kommunala myndigheter*, with no
nationality or residence condition; SCB's request instructions make the
organisation number *optional*. High-value datasets must by EU law be
available free of charge in machine-readable form to anyone. So neither EU
citizenship nor an EU company is required — a non-EU legal entity or
individual can request the certificate. Only the acceptance of SCB's user
terms is required.

## Already in hand: the derived "verksam" flag (2026-09-04 finding)

The SCB bulk file we already ingest from
`vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip` carries
`FtgStat` → `se_scb_companies.source_status_code`. That column **is** SCB's
`Företagsstatus` variable, defined in the variable description as:

> *"Anger om ett företag är verksamt. I SCB:s företagsregister betraktas ett
> företag som verksamt om det är registrerat för moms eller gruppmoms
> och/eller F-skatt och/eller som arbetsgivare."*
> 0 = har aldrig varit verksam · 1 = är verksam · 9 = ej verksam

Live distribution (2026-09-04): `1` → 1.37M companies (verksam), `0` →
327k (never active), `9` → 126k (deregistered). So the **economically-active
derivation is available today, weekly, with no API request**, for the whole
register. It has simply never been interpreted as such in our pipeline.

What the bulk file does **not** carry is the three *individual* flags
(`Fskattstatus`, `Momsstatus`, `Arbetsgivarstatus`) and their transition
semantics (e.g. "lost F-tax approval", "registered as employer for the first
time"). Those still require the API's Företag layout with the
`TG15Stat_*` add-on groups.

## What this means for the pipeline

- **Step 0 (no request needed):** expose `source_status_code` as
  `is_economically_active` (`= 1`) with its `0`/`9` distinction, and start
  recording it as a weekly observation so transitions become events. This
  alone fixes the alive-vs-dormant problem across the register.
- The individual flags and their transitions still require the API.
- With 3.4M companies and a 2,000-row/request cap at 1 request/second,
  a full sweep is ~1,700 requests ≈ 30 minutes of wall time — trivial. A
  weekly full sweep of the whole register is feasible on the free tier even
  without the paid change feed. Restrict to the active-priority tier if
  SCB's terms discourage full sweeps.
- Shape: `se_company_tax_registration_observations` (change-aware, keyed on
  org number + observed week) plus a `_current` projection; derive
  `is_economically_active = f_skatt = 1 OR moms IN (1,3) OR arbetsgivare IN
  (1,2,3,4)`. Because the free tier has no history, **our own weekly
  observations become the history** — the same discipline as the address
  and industry observation tables.
- Establishments arrive from the same API: do them in the same module.

## Recommended actions

1. Email **scbforetag@scb.se** to request free-API access (organisation,
   contact, intended layout: company-level tax flags + establishment
   fields). This is the only blocking step and it is administrative.
2. On receipt, read the accepted user terms specifically for redistribution
   in a commercial product; record the answer in the source design doc.
3. Build `sweden_scb_register_api` per `source-design-doc-template.md`:
   weekly sweep, observation tables, `_current` projections, the
   economically-active derivation, and establishment ingestion.
4. Drop the Skatteverket routes from the plan; revisit only if Skatteverket's
   investigation produces a general-purpose API (none planned as of today).

## Sources

- Skatteverket, API development — Företagsuppgifter:
  https://www.skatteverket.se/omoss/digitalasamarbeten/utvecklingsomraden/foretagsuppgifter.4.339cd9fe17d1714c0773a24.html
- Skatteverket, Allmänna villkor för API Beskattningsengagemang (Bilaga A):
  https://www7.skatteverket.se/portal-wapi/open/apier-och-oppna-data/utvecklarportalen/v1/getFile/allmanna-villkor-beskattningsengagemang
- Skatteverket, Hämta företagsinformation (e-service):
  https://www.skatteverket.se/privat/etjansterochblanketter/allaetjanster/tjanster/hamtaforetagsinformation.4.3810a01c150939e893f3e69.html
- SCB, Avgiftsfria uppgifter i företagsregistret:
  https://www.scb.se/vara-tjanster/bestall-data-och-statistik/foretagsregistret/avgiftsfria-uppgifter-i-foretagsregistret/
- SCB, Variabelbeskrivning API – SCB:s allmänna företagsregister (PDF):
  https://www.scb.se/contentassets/8a8eb5c3d45f461ea93482f8e8d4de4f/variabelbeskrivning-api.pdf
- SCB, Företagsregistrets tjänster (paid services list):
  https://www.scb.se/vara-tjanster/bestall-data-och-statistik/foretagsregistret/foretagsregistrets-tjanster/
