# Argentina (AR) — GDP Rank #23

> **Summary:** Argentina lacks a unified national free company registry API. Registration is handled at the provincial level (IGJ for Buenos Aires, DPPJ for others). AFIP CUIT (tax ID) is the universal identifier but has no fully public lookup API. Commercial providers Nosis and Veraz/Equifax are the main programmatic options.

## Official Registry

### IGJ — Inspección General de Justicia (Buenos Aires)
| Field      | Detail |
|------------|--------|
| URL        | https://www.argentina.gob.ar/justicia/igj |
| Access     | Web portal / Paid document retrieval |
| Cost       | Free search; paid certified documents |
| Fields     | Company name, legal form, CUIT (11-digit), registration number, registered address, directors, shareholders, capital, registration date, status |
| Auth       | None for search |
| Rate limit | Undocumented |
| Notes      | IGJ covers corporations registered in the City of Buenos Aires (including most large national companies). Provincial registries (DPPJ) handle other provinces. No bulk API. Searching by CUIT, name, or registration number. "Búsqueda de Personerías Jurídicas" portal provides basic data. |

### AFIP — CUIT/CUIL/CDI Registry (Tax Authority)
| Field      | Detail |
|------------|--------|
| URL        | https://www.afip.gob.ar |
| Access     | Limited (partially public) |
| Cost       | Free (constancia de inscripción) |
| Fields     | CUIT (11-digit, format: XX-XXXXXXXX-X), company name, tax status, activities (CIIU), fiscal address, VAT category |
| Auth       | Tax authority authentication required for full lookup; public constancia accessible with CUIT |
| Rate limit | N/A |
| Notes      | CUIT is the universal Argentine business/individual tax ID. Format: 20/23/27/30/33/34-XXXXXXXX-X (where 30/33/34 are for legal entities). Constancia de inscripción (tax registration certificate) is publicly accessible at https://www.afip.gob.ar/sitio/externos/default.asp. No public bulk API for company database. |

### Open Data / Datos Argentina
| Field      | Detail |
|------------|--------|
| URL        | https://datos.gob.ar |
| Access     | Bulk download (partial, free) |
| Cost       | Free |
| Fields     | Government contractors, public sector companies, some registry fragments |
| Auth       | None |
| Rate limit | N/A |
| Notes      | Some company-related datasets available (e.g., government procurement suppliers with CUIT) but not a general company registry. |

## Commercial Providers

### Nosis
| Field      | Detail |
|------------|--------|
| URL        | https://www.nosis.com |
| Access     | API (paid) |
| Cost       | Paid — per-query pricing |
| Fields     | CUIT, company name, tax status, credit score, payment history, judicial records, address, directors, AFIP filings |
| Auth       | API key |
| Rate limit | Per plan |
| Notes      | Argentina's leading KYC/KYB data provider. Strong integration of AFIP + IGJ + credit data. Most developer-friendly API for Argentine company lookups. |

### Veraz / Equifax Argentina
| Field      | Detail |
|------------|--------|
| URL        | https://www.veraz.com.ar |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | CUIT, credit history, financial behaviour, debts, judicial enforcement, company profile |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Dominant credit bureau for individuals and businesses. Required for many lending/KYC workflows. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Coverage limited; sources from IGJ and provincial registries; completeness uncertain |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Bolsa de Comercio de Buenos Aires listed companies partially covered; overall LEI adoption low |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Nosis API for programmatic KYB; AFIP constancia for basic CUIT verification
- Priority: Medium
