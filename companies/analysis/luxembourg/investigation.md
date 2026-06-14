# Luxembourg — Company Open Data Investigation

## Conclusion

Luxembourg is a **partial-open** country. The authoritative register, the **RCS (Registre de Commerce et des
Sociétés)** run by **Luxembourg Business Registers (LBR)**, is **publicly searchable for free** and lets anyone
**download many filed documents for free** — including **annual accounts (comptes annuels)** — but there is **no
open bulk export and no open API**, the search UI is **captcha-gated**, and certified extracts are paid.
Everything joins on the **RCS number** (e.g. `B123456`) and the **matricule** (13-digit national identification
number).

## What was verified (live)

- **LBR / RCS** `www.lbr.lu` → HTTP 200; the public search `.../mjrcs/jsp/secured/RechercheController.action` →
  HTTP 200. The page is the State of Luxembourg online-service login/search and references a **captcha** and
  login → the search is **captcha-protected**; there is no documented open API.
- **RESA** (Recueil Électronique des Sociétés et Associations, the legal gazette that replaced the Mémorial C)
  `www.lbr.lu/resa/` → HTTP 200. Free legal publications referencing the RCS number.
- **data.public.lu** (uData open-data API) → working. Search `entreprises` = **71 datasets**, all **STATEC
  statistical aggregates** (enterprise demography, structural business statistics, creations/cessations). Searches
  for `RCS`, `registre commerce`, `LBR`, `société`, `TVA` returned **0** company-register datasets → **no open
  RCS register or financials bulk** on the portal.
- **OpenSanctions** `lu_rcsl` → 404 (no FollowTheMoney mirror to lean on). **OpenCorporates** has the LU register
  (115).

## Identifiers

- **RCS number** — register identifier; prefix encodes the entity class (e.g. **B** = sociétés / companies,
  **A** = personnes physiques / sole traders, **F** = succursales / branches), followed by digits (e.g.
  `B123456`).
- **Matricule** (numéro d'identification national) — 13 digits (e.g. `20152411234`); the national id, doubling as
  the tax-side identifier.
- **VAT** — `LU` + 8 digits; separate from the RCS number and matricule → VIES/AED.

## Financial data

- Companies file **annual accounts (comptes annuels)**: **bilan** (balance sheet) + **compte de profits et
  pertes** (P&L) + annexes, via the structured **eCDF** electronic format. They are **public and free to
  download** on the company's RCS page, usually as **PDF**. Small companies file **abridged** accounts. Currency
  **EUR**.
- There is **no open bulk export** of the figures; the structured eCDF data is collected centrally (Centrale des
  bilans, STATEC/BCL) but **not published openly**. Structured financials at scale therefore need OCR/parsing of
  the PDFs or a **commercial provider**.

## Recommended ingestion

No lawful open bulk/automation path. Options: (a) **manual** RCS lookups (basic info + free document downloads,
incl. annual accounts); (b) a **commercial provider** (Kyckr, Creditreform, …) for the register + structured
financials at scale. Use **RESA** for events/history; **VIES/AED** to validate VAT.

## Risks / open questions

- **Access controls**: the RCS search is captcha-gated — must not be bypassed; no open API.
- **No open bulk**: data.public.lu has only STATEC statistical aggregates, not the register.
- **Financials**: free to download per company but **PDF** (no open structured bulk).
- **License**: RCS reuse/redistribution terms are not clearly stated — confirm before redistribution.
- **RBE** (beneficial ownership) is **restricted** (post-CJEU). Officer/owner data = GDPR.
