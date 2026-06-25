# Search attempts — Ghana

## Attempt 1
- Date/time: 2026-06-25
- Source: direct probe of candidate official hosts
- Query: GET `orc.gov.gh`, `rgd.gov.gh`, `eservices.rgd.gov.gh`, `gse.com.gh`,
  `data.gov.gh`, `gra.gov.gh`
- Language: English
- Result: only gse.com.gh responded (200); all .gov.gh hosts timed out (000)
- Decision: check DNS; pursue GSE

## Attempt 2
- Date/time: 2026-06-25
- Source: DNS + retry
- Query: `host orc.gov.gh` / `rgd.gov.gh`; GET with browser UA
- Result: **DNS resolves** (orc.gov.gh → 197.253.124.98; rgd → 197.253.67.105) but
  HTTP **times out** → network block from this environment
- Decision: document ORC/RGD/GRA from public knowledge; mark firewalled

## Attempt 3
- Date/time: 2026-06-25
- Source: GSE (`gse.com.gh`)
- Query: home; `/listed-companies/`; `/financial-statements/`
- Result: **OPEN** — listed-company directory returns real companies (Ecobank Ghana
  PLC, GCB/Ghana Commercial Bank, AngloGold Ashanti, CalBank, Standard Chartered
  Ghana, Guinness Ghana, Fan Milk, Enterprise Group...); financial-statements page 200
- Decision: GSE = recommended (open, listed)

## Attempt 4
- Date/time: 2026-06-25
- Source: identifiers / tax
- Query: company registration number; TIN; Ghana Card PIN
- Result: company registration number (ORC), TIN (GRA, businesses), Ghana Card PIN
  (individuals), business registration (sole props)
- Decision: document identifier model
