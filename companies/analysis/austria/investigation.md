# Austria — Company Data Investigation

Date: 2026-06-14
Investigator: company-open-data-discovery skill (Claude)
Country code: AT | Languages: German, English

## Goal

Find official/reliable public sources for Austrian company data — prioritising bulk-ingestible open data
and **financial data** (Jahresabschluss) — and leave a reproducible trail with samples and licensing.

## Summary of findings

Austria is a **paid-register** country, similar to Germany/Italy: the authoritative **Firmenbuch** and the
**annual accounts (Jahresabschluss)** are accessed mostly **for a fee** via authorized clearing houses
(**Verrechnungsstellen**). There is **no open bulk company master and no open bulk financials**. The open
layer is **GISA trade authorizations** (data.gv.at), a **free brief Firmenbuch extract**, an
**Austrian-ID-gated** JustizOnline API, and the **free insolvency gazette** (Ediktsdatei).

### 1. Firmenbuch — authoritative, mostly PAID
- Publisher: courts / **Bundesministerium für Justiz (BMJ)**. The authoritative register of companies
  (GmbH, AG, OG, KG, Genossenschaften, …). Keyed on the **Firmenbuchnummer** (e.g. `FN 123456a`).
- Access:
  - **Free**: a brief **"aktueller Teilauszug"** + short info via **JustizOnline** (justizonline.gv.at).
    Limited fields per company.
  - **Free JustizOnline API** to Firmenbuch data exists, **but requires an Austrian ID** (ID Austria /
    Handy-Signatur) to register — not usable for foreign/automated open ingestion without an AT identity.
  - **Paid**: full **Firmenbuchauszug**, documents, and **Jahresabschluss** via **Verrechnungsstellen**
    (clearing houses commissioned by the BMJ: Compass, KSV1870, HF data, Lexunited, Manz, …). These offer
    complex search, bulk usage, and document purchasing. **No open bulk.**

### 2. Jahresabschluss — FINANCIALS (paid)
- GmbH/AG must file annual accounts to the **Firmenbuch Urkundensammlung** (document archive). These are
  **publicly accessible for a fee** via the clearing houses. Since **1.1.2026** filing is via JustizOnline
  (no longer FinanzOnline).
- Documents are filed (PDF / structured electronic filing); **no open machine-readable bulk**. The realistic
  path to financials at scale is a **commercial aggregator** (Compass, KSV1870, **firmafind.at** — which
  exposes Firmenbuch + Jahresabschluss as JSON) or paid per-document retrieval.

### 3. GISA — Gewerbeinformationssystem — OPEN (trade authorizations)
- The trade-licence register. **Free online queries** at gisa.gv.at/abfrage (no registration).
- **Open dataset on data.gv.at: "Gewerbe in Österreich"** — **active trade authorizations
  (Gewerbeberechtigungen) WITHOUT personal data**, derived from GISA. Resources: **GISA CSV + GISA JSON**
  (+ monthly statistics). Fields include business name, location, **GISA-Zahl**, and the trade wording
  (Gewerbewortlaut). Open license.
- Caveat: it is **trade licences, not a company master**, and it **excludes natural-person sole traders'
  personal data**. Still the best **open** per-business artifact.
- Note: the actual file is hosted behind the **data.gv.at JS portal**; the direct download URL could not be
  resolved in this environment (the CKAN API path returned the SPA HTML and guessed resource URLs 404'd).
  Documented for follow-up; the dataset and its resources are confirmed to exist and be open.
- A companion **trade-code list** (Gewerbeschlüssel / standardisierte Gewerbewortlaute) is open on data.gv.at.

### 4. Ediktsdatei / Insolvenzdatei — insolvency gazette (free queries; structured feed licensed)
- edikte.justiz.gv.at: insolvency proceedings, judicial auctions, and register announcements. **Free to
  query** (web). Useful for status/lifecycle (insolvency) keyed by company name/Firmenbuchnummer.
- A structured **JSON feed** exists at `iwg.justiz.gv.at/edikte/…` but requires an **IWG re-use licence**
  (Informationsweiterverwendungsgesetz) — confirmed login wall. Free access is web-query only without it.

### 5. WiEReG — beneficial ownership (restricted)
- Register der wirtschaftlichen Eigentümer. Access is **restricted/fee-based** — not open bulk. Out of scope.

### 6. Statistik Austria + data.gv.at + commercial
- **Statistik Austria** Unternehmensregister — statistical, **aggregate** (not per-company open).
- **data.gv.at** — national open-data catalog (hosts the GISA datasets + trade codes).
- **Commercial aggregators**: Compass, KSV1870, Bisnode/Dun & Bradstreet, firmafind — paid full master +
  financials + documents.

## Conclusion

- **Authoritative + financials**: Firmenbuch + Jahresabschluss — **paid** (clearing houses / aggregators).
  No open bulk.
- **Free open**: GISA trade authorizations (data.gv.at), free brief Firmenbuch extract, free insolvency
  gazette queries. Austrian-ID holders also get a free JustizOnline Firmenbuch API.
- **At scale**: a commercial aggregator (Compass/KSV1870/firmafind) is the realistic route for a full
  company master with financials.

## Risks / open questions

- **No open per-company master, no open bulk financials** — paid clearing house / aggregator needed.
- **GISA open file URL** could not be resolved in this environment (data.gv.at SPA) — resolve via the
  portal UI / the dataset's CKAN resource before ingestion; confirm exact columns + license.
- **JustizOnline API** is free but **ID-Austria-gated** — only usable with an Austrian identity.
- **Insolvency structured feed** needs an **IWG licence**; free access is web-query only.
- **Identifiers**: Firmenbuchnummer (FN + check letter), UID (ATU########), GISA-Zahl — reconcile across
  sources; GISA open data has no Firmenbuchnummer link guaranteed.
- **License**: GISA/data.gv.at open (confirm attribution); Firmenbuch/Jahresabschluss contractual.
