# Sweden — proposed new data sources and priority

Status: proposal, 2026-09-04. Nothing here is built. This document complements
`sweden-data-sources.md` (the map of what is live) — that file's "what should
be done next" list covers data-quality work on *existing* sources; this one
covers *new* sources, ranked by product value per unit of effort.

Entity key for everything: the 10-digit organisationsnummer. A source is only
worth adding if its records resolve to that key (directly, or through a
domain↔company link we already maintain).

## What Sweden already covers

Register (Bolagsverket + SCB bulk), annual reports (inline XBRL) and ESEF,
VDM company/financial documents, Ratsit enrichment, Wikidata, addresses +
geocoding, SNI industries, people/roles/signatories, audits, proceedings
(liquidation/bankruptcy/restructuring), Platsbanken + JobTech job ads, UHM
procurement and government contracts, and the cross-country web/DNS/technology
layer (CommonCrawl, DNS records, technology detections with evidence windows).

The gaps that matter are therefore not "more company attributes" but three
specific kinds of fact the dossier cannot state today:

1. **Is this company economically alive right now?** (register status lags
   reality by months; ~half the register is dormant or shell entities)
2. **Is it in financial trouble before the annual report says so?**
3. **What is it *doing* — launching, innovating, expanding — as dated events?**

The ranking below follows those three questions.

## Tier 1 — build first (open, structured, org-number keyed)

### 1. Skatteverket registration status — F-skatt, moms, arbetsgivare

Three yes/no flags per company: approved for F-tax, VAT-registered,
registered as employer. Together they are the best available discriminator
between an operating company and a dormant/shell one — far better than
Bolagsverket status, which stays "active" for years after activity stops.
Employer registration also gives a rough "has staff" signal for companies
that have not filed a report yet.

- Access: Skatteverket public company lookup / open API; per-company queries
  are permitted, bulk terms to be confirmed. Low volume: three flags per org
  number, refreshed monthly.
- Value: fixes the "alive vs dormant" problem for the *entire* register,
  which improves every downstream count, adoption statistic, and priority
  score. Directly feeds the company-priority tiering.
- Effort: small. One Dagster asset, one narrow table
  (`se_company_tax_registration_observations`, change-aware like the other
  observation tables).

### 2. Kronofogden — enforcement cases and payment defaults

Betalningsförelägganden, active enforcement cases, and the enforcement-debt
balance. This is the anchor of every Swedish credit score (UC, Creditsafe,
Bisnode build on it) and the strongest *short-term* distress signal that
exists — it moves months before the annual report.

- Access: public records; per-company lookup available, bulk/commercial
  access via agreement with Kronofogden. The commercial-use terms must be
  settled before it ships in a sold product.
- Value: the KYB/vendor-risk lane's core input; also a sales-lane filter
  ("exclude accounts with active enforcement"). As events, "new enforcement
  case opened" is a high-signal trigger.
- Effort: medium. Access negotiation is the long pole, not the pipeline.

### 3. SCB Företagsregistret — establishments and group structure

Two datasets from a register we already ingest:

- **Arbetsställen (establishments)**: every workplace with address and
  size band. Turns "the company" into "its N locations", the natural join to
  the geocoding layer, and the only reliable per-location headcount proxy.
- **Koncernstruktur (group structure)**: parent/subsidiary trees. Roughly
  800k register rows collapse to ~200k economic groups; this is what makes
  "who actually owns this company" answerable and de-duplicates hiring,
  technology, and financial signals across a group.

- Access: SCB open/paid extracts; the company bulk we take already comes
  from SCB, so the relationship exists.
- Value: entity dedup + geography. Group structure also unlocks
  parent-level financials for subsidiaries that file nothing meaningful.
- Effort: medium. Group trees need a small graph model
  (`se_company_group_links` with effective dates).

### 4. Bolagsverket — the parts we do not take yet

The register bulk is in; three adjacent Bolagsverket products are not:

- **Verklig huvudman (beneficial ownership)** — required for any KYB use.
- **Företagsinteckningar (business mortgages/charges)** — a debt/leverage
  signal absent from small-company reports.
- **The events feed** — name changes, board changes, share issues, address
  changes as dated events. This is *the* trigger source for Sweden: every
  change is a fact with a date and an org number.

- Access: Bolagsverket subscriptions/APIs (some paid, terms known and modest).
- Value: events power the trigger product; ownership and charges power risk.
- Effort: small–medium each; the events feed is the highest value of the
  three.

### 5. PRV — trademarks and patents

Filings keyed by applicant org number. Patents indicate R&D activity; a new
trademark filing is a product-launch or rebrand trigger, typically months
before anything public.

- Access: PRV open data / Svensk Patentdatabas, fully public.
- Value: the clean "innovating vs not" indicator; combines with the planned
  AI-adoption score (a company filing ML-related patents is Tier-1 evidence).
- Effort: small. Bounded volume, simple schema
  (`se_company_ip_filings`).

## Tier 2 — high value, moderate effort

### 6. Mynewsdesk / Cision press releases

The Nordic standard for company news; most Swedish companies above a few
dozen employees publish there. Structured, dated, attributable to the
publishing company — the "news" source that avoids every problem of social
or forum crawling (entity resolution, licensing, noise). Funding, M&A, new
CEO, product launches as dated events.

- Access: public pages/RSS per company; commercial API terms to confirm for
  redistribution.
- Effort: medium (matching publisher → org number is the work).

### 7. Listed-company layer — Nasdaq Stockholm, First North, Spotlight, FI registers

Listing status and instrument, plus Finansinspektionen's open
insider-transaction and net-short-position registers. Only ~1,000 companies,
but with near-real-time, high-quality events. LEI/ESEF linkage already
exists for these issuers.

- Effort: small–medium; FI data is open CSV/API.

### 8. Sector regulators — environmental permits and workplace inspections

Länsstyrelsen/Naturvårdsverket environmental permits; Arbetsmiljöverket
inspection outcomes. Org-number keyed, public. Narrow (manufacturing,
construction, energy) but strong ESG/risk evidence for those sectors.

### 9. Lantmäteriet — company-owned property

We already hold Lantmäteriet credentials. Company-owned real estate is a
balance-sheet and locality signal, and ownership transfers are events.

## Tier 3 — later

### 10. Public funding — Vinnova, Tillväxtverket, EU funds

Grant recipients with amounts and dates: a "growth-stage / innovation-active"
marker. Open data, small volume.

### 11. Municipal direct awards

Fragmented across municipalities; UHM + TED already cover the formal tiers.
Low priority until the procurement lane has customers asking.

### 12. .se zone / domain registration data (IIS)

Registration dates and registrant organisations for .se domains. Modest
signal on its own ("got its domain on date X") but materially improves
domain↔company resolution, which gates every web-side signal for companies
outside the current match set.

## Explicitly not recommended

- **Social/forum sentiment (X, Reddit, general forums)** — closed or
  expensive access with redistribution limits, near-impossible entity
  resolution for non-brand companies, and opinion rather than fact. It fails
  the test every other source here passes: *a dated fact, attributable to
  an org number, that a customer would act on.* Structured review platforms
  and the adverse-record registries above give the "what others say" signal
  in a form that is reliable and sellable.

## Recommended order

Do these three first. They do not add a new headline signal; they make every
existing signal more trustworthy, which is the better investment while the
platform's coverage and priority model are being settled:

1. **Skatteverket status flags** — cheapest, fixes "alive vs dormant" across
   the whole register, immediately improves priority tiering and every
   aggregate statistic.
2. **SCB establishments + group structure** — fixes entity dedup and
   geography; parent/subsidiary trees are a prerequisite for correct
   group-level counts in the technology, hiring, and financial rollups.
3. **Kronofogden** — the risk-lane anchor; start the access negotiation
   early because it is the long pole.

Then, in order: **Bolagsverket events feed** (the trigger product's Swedish
backbone), **PRV filings** (innovation + AI-adoption evidence), **press
releases**, then the listed-company layer and sector regulators as the risk
lane matures.

## Cross-cutting requirements for every new source

These follow `data-source-guidelines.md` and the existing Swedish modules;
listed here because they decide whether a source is worth adding at all:

- **Org-number resolution or nothing.** A source whose records cannot be
  keyed to organisationsnummer (directly or via a maintained domain link)
  does not ship.
- **Observations, not snapshots.** Every source lands as change-aware,
  dated observation rows with a `_current` projection — the same shape as
  addresses, industries, and proceedings — so it feeds timelines and
  triggers, not just the detail page.
- **Evidence retained.** Keep the source record id/URL so any derived flag
  or score can show the row that proves it.
- **Licensing settled before productization.** Skatteverket, Kronofogden,
  SCB group data, and Bolagsverket subscriptions all have terms that differ
  between internal enrichment and redistribution in a sold product.
  Confirm the commercial-use terms as part of the source design doc, not
  after launch.
- **Priority-aware scheduling.** New per-company lookups (Skatteverket,
  Kronofogden) should read their candidate list from the planned
  company-priority tiering rather than scanning the full register on every
  run.
- **Design doc first.** Each source gets a doc from
  `source-design-doc-template.md` before code.
