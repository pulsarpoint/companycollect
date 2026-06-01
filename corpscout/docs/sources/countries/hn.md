# Honduras (HN) — GDP Rank #98

> **Summary:** Commercial registrations in Honduras are handled by regional Cámaras de Comercio (Chambers of Commerce) with no unified national registry. RTN (tax ID) is the primary business identifier managed by SAR. No unified public API exists.

## Official Registry

### SAR (Servicio de Administración de Rentas) — RTN Lookup
| Field      | Detail |
|------------|--------|
| URL        | https://www.sar.gob.hn |
| Access     | Portal search |
| Cost       | Free |
| Fields     | RTN (Registro Tributario Nacional — tax ID), taxpayer name, type, status |
| Auth       | None for basic lookup |
| Rate limit | Low |
| Notes      | Tax authority RTN verification; RTN is the primary business identifier; no unified company registry at national level |

### CCIT (Cámara de Comercio e Industria de Tegucigalpa)
| Field      | Detail |
|------------|--------|
| URL        | https://www.ccit.hn |
| Access     | Portal search |
| Cost       | Free |
| Fields     | company name, registration number, legal form, status |
| Auth       | None |
| Rate limit | Low |
| Notes      | Tegucigalpa Chamber of Commerce handles registrations for the capital region; no national unified registry; separate chambers for other regions (San Pedro Sula, etc.) |

## Commercial Providers

### Dun & Bradstreet (Central America)
| Field      | Detail |
|------------|--------|
| URL        | https://www.dnb.com |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | name, D-U-N-S, RTN, address, sector |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Very limited Honduras coverage |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Minimal Honduras coverage |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed companies only; very few Honduras LEIs |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: SAR RTN portal for tax ID lookups; CCIT portal for Tegucigalpa registrations
- Priority: Low
