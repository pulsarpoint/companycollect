# Brazil RFB Contact Domains Design

## Goal

Add a Brazil contact/domain layer that preserves all registry contact values and feeds email-derived company domains into the existing cross-source domain graph.

## Source Shape

Brazil RFB separates legal entities (`Empresas`) from establishments (`Estabelecimentos`). The contact fields live on establishments, so the contact layer is derived from `br_establishments` rather than `br_companies`.

The canonical contact table is `corpscout.br_company_contact_info`. It stores one row per non-empty contact value from the establishment table:

- `email` from `correio_eletronico`
- `phone` from `ddd_1` + `telefone_1`
- `phone` from `ddd_2` + `telefone_2`
- `fax` from `ddd_fax` + `fax`

Each row keeps `cnpj`, `cnpj_basico`, source fields, contact type, contact value, current flag, and optional email domain metadata.

## Email-Domain Rule

Brazil uses the same rule as Estonia. An email suffix becomes a company domain only when:

- the email value contains a syntactically usable domain suffix,
- the suffix contains a dot,
- the suffix is used by exactly one distinct `cnpj_basico`, and
- the suffix is not in a small public-provider denylist.

The distinct-company rule removes public email providers and shared accountant/formation-agent domains without using a confidence score.

## Domain Feeder

`corpscout.br_websites` is the Brazil feeder table for the cross-source domain graph. The first version contains email-derived domains, not official websites, because RFB does not publish website URLs.

The table is deduped to one row per `(cnpj_basico, root_domain)`. Email-derived rows have:

- `domain_source = 'email'`
- empty `website_url`
- empty `website_normalized_url`
- empty `website_host`
- one `is_primary = 1` row per `cnpj_basico`

## Graph Integration

The existing `domains_clickhouse` asset adds a `br_websites` branch to `company_website_domains`:

- `source_website_table = 'br_websites'`
- `source_slug = 'brazil_rfb'`
- `company_id_type = 'cnpj_basico'`
- `company_id = cnpj_basico`
- `root_domain = br_websites.root_domain`
- `domain_source = br_websites.domain_source`

`company_website_domains` then continues to feed the existing `domains` aggregate.

## Implementation Boundary

Keep this in plain DuckDB SQL inside the Brazil RFB Dagster package. Do not introduce dbt for this phase. dbt can be revisited when there are enough reusable cross-country semantic models to justify the added project and integration layer.
