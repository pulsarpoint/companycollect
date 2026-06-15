# Slovenia — License Notes

## AJPES PRS (business register, via OPSI)

- **License: CC-BY 4.0** (Attribution). Free reuse. Attribute "AJPES — Poslovni
  register Slovenije" and retain source + retrieval date.
- Refreshed twice monthly. The companion `restPrsInfo` web service is **separate**
  and **credentialed** (AJPES terms; not for mass download) — do not conflate it
  with the open CSV.
- Encoding: **UTF-16** (handle BOM + 2-byte chars on ingest).

## FURS — Seznam davčnih zavezancev (via OPSI)

- **License: CC-BY 4.0** (Attribution). Free reuse. Attribute "FURS — Finančna
  uprava RS". Updated daily.
- Contains **davčna številka** and **matična številka** of legal entities plus VAT
  status, SKD activity, name, address. These are **business identifiers of legal
  persons** (not personal data); however the natural-persons / sole-trader lists
  (DEJ, FO) concern individuals — treat those as personal data if used.

## AJPES JOLP (annual reports)

- **Free to view** per company, but the **reuse/redistribution terms are not the
  same as the CC-BY open datasets**. There is no open bulk/API; do not scrape
  JOLP en masse. Treat structured financials as **view-only**.

## AJPES Fi=Po / S.BON

- **Paid** (contract/login). Fi=Po (structured financial statements + indicators)
  and S.BON (credit ratings) are commercial products — **planning-only**; no raw
  values copied.

## Summary

- **Open & usable (CC-BY 4.0)**: PRS (identity) + FURS (tax/VAT/activity) — attribute.
- **Credentialed**: restPrsInfo (broader fields; not mass download).
- **View-only**: JOLP financials.
- **Paid**: Fi=Po / S.BON.
- **Personal-data caution**: FURS natural-persons lists; sole traders (s.p.).
