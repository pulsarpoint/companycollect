# Portugal — Schema Notes

No per-company open record was lawfully downloadable (register paid; publicacoes.mj.pt reCAPTCHA-gated;
dados.justica = statistical aggregates). Fields below are documented from the Registo Comercial / certidão
permanente data model and the IES. Join on the **NIPC** across sources.

## Identifiers
- **NIPC** (Número de Identificação de Pessoa Coletiva) — 9 digits; the company id and join key. For companies it
  **equals the NIF** (tax number). **VAT** = `PT` + NIPC.
- **Número de matrícula** — commercial-registration number (frequently equal to the NIPC).
- **CAE** (Classificação Portuguesa das Atividades Económicas, Rev.3) — activity code (NACE-aligned).
- Language: **Portuguese**.

## Registo Comercial / certidão permanente — documented fields
```
nipc                 - 9-digit NIPC (company id / tax number)
firma / denominação  - company name
sede                 - registered office address
cae                  - activity code (CAE Rev.3)
natureza juridica    - legal form (Lda. / S.A. / Unipessoal Lda. / Cooperativa / ...)
objeto               - corporate purpose
capital social       - share capital (EUR)
estado               - status (ativa / dissolvida / insolvente / cancelada)
data de constituicao - incorporation date
socios               - shareholders [PII; paid]
gerencia/administracao - management/officers [PII; paid]
```

## publicacoes.mj.pt — company acts (free; reCAPTCHA)
```
nipc, firma, tipo de ato (constituição / alterações ao contrato / dissolução / ...), data de publicação, texto do ato
```

## IES (Informação Empresarial Simplificada) — annual accounts (not openly published)
```
balanço (balance sheet): ativo, capital próprio (equity), passivo (liabilities)
demonstração de resultados (income statement): volume de negócios (turnover), resultado líquido (net result)
anexo (notes) ; CAE ; n.º de trabalhadores (employees)
standard: SNC (Sistema de Normalização Contabilística) ; currency EUR
```
- Filed to AT/INE/Banco de Portugal + the commercial register (depósito de contas). Per-company figures via the
  paid register or a commercial provider.

## Mapping to internal company model
```
company_id          <- nipc
registration_number <- nipc (número de matrícula)
nipc                <- NIPC (9 digits)
tax_id              <- NIPC (= NIF for companies)
vat_id              <- PT + NIPC (VIES)
legal_name          <- firma / denominação
company_type        <- natureza jurídica (Lda./S.A./Unipessoal Lda./...)
status              <- estado (ativa/dissolvida/insolvente)
incorporation_date  <- data de constituição
registered_address  <- sede
municipality        <- from address (concelho)
activity_code       <- CAE (Rev.3)
share_capital       <- capital social (EUR)
officers[]          <- gerência/administração [PII; paid]
shareholders[]      <- sócios [PII; paid]
financials[]        <- IES (balanço + demonstração de resultados) [EUR; not open -> paid register / provider]
beneficial_owners[] <- RCBE (restricted) [PII]
country             <- "Portugal"
source_url/name/at, raw_record
```
See `companies/data/portugal/normalized/companies.sample.jsonl` (schematic — no per-company open record was
lawfully downloadable here).
