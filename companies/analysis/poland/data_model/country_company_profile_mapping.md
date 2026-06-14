# Poland Company Profile — Source Mapping

How each section of `country_company_profile.schema.json` is populated. **Poland's defining trait:
everything is OPEN and bridged by clean identifiers** — KRS (companies), NIP (universal), REGON — joined
in one white-list lookup. Identity, financials, and beneficial ownership are all free.

## Identity / legal / activity / location

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| registration.krs | krs_api / vat_whitelist | numerKRS / subject.krs | **PK (companies)** | continuous/daily | open | null for sole traders |
| registration.nip | krs_api / vat_whitelist / ceidg | identyfikatory.nip / subject.nip | **universal bridge** | continuous/daily | open | VAT = PL+NIP |
| registration.regon | krs_api / vat_whitelist | identyfikatory.regon / subject.regon | bridge | continuous/daily | open | normalize 14→9 digit |
| entity_kind | krs_api / ceidg | — | — | — | open | company vs sole_proprietor |
| legal_identity.nazwa | krs_api (else white list/CEIDG) | danePodmiotu.nazwa | — | continuous | open | KRS authoritative |
| legal_identity.forma_prawna | krs_api | danePodmiotu.formaPrawna | — | continuous | open | |
| status.derived | krs_api dzial6 / ceidg status | likwidacja/upadłość / ZAWIESZONY | — | continuous | open | + suspended for sole traders |
| status.vat_status | vat_whitelist | subject.statusVat | — | daily | open | Czynny/Zwolniony |
| activity.pkd_* | krs_api (else CEIDG) | dzial3 PKD | — | continuous | open | **clean codes** |
| registered_location.* | krs_api (else white list) | siedzibaIAdres | — | continuous | open | region = wojewodztwo |
| capital.* | krs_api | dzial1.kapital | — | continuous | open | register capital, PLN |
| contact.website | krs_api | adresStronyInternetowej | — | continuous | open | |
| contact.bank_accounts | vat_whitelist | subject.accountNumbers | — | daily | open | unique to white list |
| officers[] | krs_api | dzial2.reprezentacja | krs | continuous | open (anonymized) | board; PII anonymized by source |
| beneficial_owners[] | crbr_beneficial_ownership | beneficjenci[] | nip | continuous | open · **sensitive PII** | minimize PESEL |

## Financial statements (open, structured)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| financial_statements[] | krs_rdf_financials | bilans + rachunek zysków i strat | krs | annual | **open** | Free structured XML; trigger from KRS wzmianki |
| filing_signals.wzmianki… | krs_api | dzial3.wzmiankiOZlozonychDokumentach | krs | continuous | open | which periods filed → fetch RDF |

### Financial precedence
- **Single open source**: `krs_rdf_financials` (e-Sprawozdania XML). No paid tier needed — a Poland
  advantage over DE/IT/ES. Dedupe on `krs + period_end + entity_type`; revenue/net_income/employees
  nullable for mikro/małe; scale by `jednostka` (whole vs thousands); store currency (PLN).

## Join & precedence summary

- **Clean multi-key, all open**: KRS (companies), NIP (universal), REGON. The **white list** returns all
  three in one call — the canonical bridge. Sole traders (CEIDG) have **no KRS** → key on NIP.
- **Authority**: KRS authoritative for legal identity/status/capital/activity/officers; white list for VAT
  status + bank accounts + the id bridge; RDF for financials; CRBR for beneficial ownership.
- **Build order**: KRS spine → white list (bridge + VAT + accounts) → RDF (financials, triggered by KRS
  wzmianki) → CRBR (owners) → CEIDG (sole traders). Freshness: KRS/CRBR continuous, white list/CEIDG daily,
  RDF annual.

## Missing / restricted data — minimal

- Almost nothing is missing: identity, financials, ownership, VAT, bank accounts are **all open**.
- **PII handling**, not availability, is the constraint: CEIDG entrepreneur names and **CRBR beneficial
  owners (incl. PESEL)** are personal data — GDPR minimization required. KRS board members are anonymized
  by the source.
- **No single full bulk** — enumerate KRS numbers or seed from the white-list daily flat file.
- **REGON length** (14 vs 9) and **financial XML schema versions/units** need normalization.
