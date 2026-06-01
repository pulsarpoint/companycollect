# Brazil (BR) — GDP Rank #10

> **Summary:** Receita Federal's CNPJ bulk download is one of the most open and comprehensive free datasets in the world — monthly full dumps covering all ~55M registered CNPJs including razão social, fantasy name, CNAE, address, phone, email, partners, and capital. An excellent programmatic source with no authentication required.

## Official Registry

### Receita Federal — CNPJ Bulk Download
| Field      | Detail |
|------------|--------|
| URL        | https://dadosabertos.rfb.gov.br/CNPJ/ |
| Access     | Bulk download (free) |
| Cost       | Free |
| Fields     | CNPJ (14-digit), razão social (legal name), nome fantasia (trade name), situação cadastral (status), data situação, CNAE principal, CNAE secundários, logradouro (address), CEP, municipality, state, DDD + phone, email, capital social, porte (size), abertura (opening date), natureza jurídica (legal nature), quadro societário (partners/shareholders with CPF/CNPJ), responsible qualifier |
| Auth       | None |
| Rate limit | N/A (bulk) |
| Notes      | Full monthly dumps in ZIP format. ~55M entries (active + inactive). Multiple files split by CNPJ range. Also includes: establishment details (CNPJ + branch), partner data, municipal codes, CNAE codes as separate files. Python/pandas-friendly format. One of the most comprehensive free business registry datasets globally. |

### Receita Federal — CNPJ Consultation API (per-query)
| Field      | Detail |
|------------|--------|
| URL        | https://www.receitaws.com.br/v1/cnpj/{cnpj} (third-party wrapper) |
| Access     | API (free, via unofficial wrapper) |
| Cost       | Free (with rate limits) |
| Fields     | Same as bulk: nome, fantasia, situação, CNAE, address, phone, email, socios, capital |
| Auth       | None (unofficial wrappers); API key for commercial wrappers |
| Rate limit | 3 req/min on free tier of ReceitaWS |
| Notes      | The official Receita Federal website is not API-accessible; ReceitaWS and similar third-party services wrap it. For production use, the official bulk download is more reliable. |

## Commercial Providers

### Serpro (government IT company)
| Field      | Detail |
|------------|--------|
| URL        | https://dadosabertos.estaleiro.serpro.gov.br |
| Access     | API (paid) |
| Cost       | Paid — government pricing model |
| Fields     | CNPJ data, CPF data (individuals), certified CNPJ status |
| Auth       | API key via Serpro marketplace |
| Rate limit | Per plan |
| Notes      | Serpro is the federal government's IT company. Their CNPJ API is considered authoritative and real-time. Priced by consumption. Used by banks and fintechs for KYC. |

### Assertiva / Serasa Experian Brazil
| Field      | Detail |
|------------|--------|
| URL        | https://www.serasaexperian.com.br |
| Access     | API (paid) |
| Cost       | Paid — contact for pricing |
| Fields     | CNPJ, credit score, financial history, protests, judicial debts, shareholders, address |
| Auth       | API key |
| Rate limit | Per contract |
| Notes      | Serasa Experian is Brazil's dominant credit bureau. Strong for credit risk data beyond what Receita Federal provides. |

## Aggregators

### OpenCorporates
| Field  | Detail |
|--------|--------|
| Access | API (paid) |
| Cost   | Paid |
| Fields | name, number, status, address, incorporation date |
| Notes  | Sources from Receita Federal bulk data; no advantage over free direct access |

### GLEIF
| Field  | Detail |
|--------|--------|
| Access | API (free) |
| Cost   | Free |
| Fields | LEI, legal name, HQ country, parent LEI |
| Notes  | Large/listed Brazilian companies; Bovespa/B3-listed entities well covered |

## Corpscout Status
- [ ] Adapter implemented
- Source name: `—`
- Recommended source: Receita Federal CNPJ bulk download (free, monthly, comprehensive, no auth — exceptional dataset)
- Priority: High
