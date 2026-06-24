# Russia — Search Attempts

## Attempt 1

- Date/time: 2026-06-24
- Source: FNS sites (EGRUL, GIR BO, opendata) + data.gov.ru
- URL: egrul.nalog.ru ; bo.nalog.ru ; www.nalog.gov.ru/opendata/ ; data.gov.ru
- Language: Russian
- Why: FNS runs the company register (EGRUL) and the financial-statements resource (GIR BO).
- Result: EGRUL 307 -> JS app; bo.nalog.ru 302 -> bo.nalog.gov.ru (200); nalog.gov.ru/opendata 200; data.gov.ru 000 (TLS error).
- Decision: Pursue GIR BO (financials/identity) + FNS opendata; EGRUL bulk likely paid.

## Attempt 2

- Date/time: 2026-06-24
- Source: FNS opendata listing
- URL: https://www.nalog.gov.ru/opendata/
- Language: Russian
- Why: Find open company/financial datasets.
- Result: many per-INN datasets; the SME register (rsmp) and formerly-tax-secret sets (revexp, paidtax, sshr, registerdisqualified). EGRUL full bulk not among the free open sets.
- Decision: RSMP = best open company list; GIR BO = financials.

## Attempt 3

- Date/time: 2026-06-24
- Source: RSMP open dataset passport
- URL: https://www.nalog.gov.ru/opendata/7707329152-rsmp/
- Language: Russian
- Why: Get the bulk URL + structure.
- Result: bulk XML ZIPs at file.nalog.ru (latest data-10062026, ~2.25 GB) + XSD. Fields: ИННЮЛ, ОГРН, НаимОрг, Регион, СвОКВЭД, КатСубМСП, ССЧР, ДатаВклМСП. XSD downloaded.
- Decision: RECOMMENDED (open bulk SME register). Archive too large to fetch here; XSD parsed.

## Attempt 4

- Date/time: 2026-06-24
- Source: GIR BO search + financial-statements API
- URL: https://bo.nalog.gov.ru/advanced-search/organizations/search?query={INN} ; /nbo/organizations/{id}/bfo/
- Language: Russian
- Why: Open financial statements + identity.
- Result: real data — ПАО "ГАЗПРОМ" (7736050003, OGRN 1027700070518, statements 2021-2025), ПАО "ЛУКОЙЛ" (7708004767). Banks (Sberbank) excluded. Org record: INN/OGRN/KPP/OKOPF/OKFS/OKPO/OKVED/region/status.
- Decision: RECOMMENDED (open financials + identity). Used as the real sample.

## Attempt 5

- Date/time: 2026-06-24
- Source: EGRUL
- URL: https://egrul.nalog.ru/
- Language: Russian
- Why: The authoritative full register.
- Result: free per-company extract (выписка, PDF) by OGRN/INN/name; full bulk via a paid FTP subscription. JS app.
- Decision: blocked_by_payment (full bulk); GIR BO + RSMP cover most fields openly.
