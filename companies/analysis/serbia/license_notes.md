# Serbia — License and Access Notes

## APR Companies Open Data API

- Current `data.gov.rs` license identifier: `sodl`
- Current label: **Српска лиценца за отворене податке**
- Standardized identifier reported by Serbia's Open Data Hub: **SODL_1_0**
- Publisher: Agencija za privredne registre (APR)

The earlier workspace note that classified the endpoint as `public_domain` is
stale. The current official catalog page and catalog API classify it under the
Serbian Open Data License. Preserve attribution, source URL, retrieval time and
`DatumPreseka`; review the full SODL terms before redistribution.

The catalog metadata says `continuous`, while the human-readable description
explicitly says the API is updated once per month. The payload behavior and
snapshot dates support treating it as monthly.

## APR public search

The public search and the open-data API are different access surfaces. APR's
terms prohibit automatic tools against search results, and the search page says
unauthorized access by applications/scripts will be blocked. Do not use the
public search for automated collection, even though individual records are
publicly viewable.

## APR one-off delivery and automated web service

- Paid and governed by APR's service rules and, for the web service, a standard
  data-delivery contract.
- Redistribution rights are not open by default.
- State bodies may receive web-service access without a fee; banks and other
  businesses pay prescribed fees.
- Confirm retention, downstream display and redistribution rights in writing.
- Representative data concerns natural persons. Store only fields required for
  the product and avoid retaining JMBG, passport numbers, home addresses or
  other sensitive identifiers unless APR supplies them lawfully and there is a
  documented legal basis and security design.

Contact: `apr-podaci@apr.gov.rs`.
