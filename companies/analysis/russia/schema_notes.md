# Russia — Schema Notes

## Identifiers

- **ОГРН (OGRN)** — **13-digit** Primary State Registration Number — the company id
  (OGRNIP for individual entrepreneurs, 15-digit).
- **ИНН (INN)** — taxpayer id: **10-digit** for legal entities, **12-digit** for
  individuals. The tax id.
- **КПП (KPP)** — **9-digit** tax-registration reason code (differs per branch /
  registration reason).
- Russia has **VAT (НДС)** but **no separate VAT number** — the INN is the tax id.
- **Classifiers**: **OKVED** (economic activity, ~NACE), **OKOPF** (legal form),
  **OKFS** (ownership form), **OKPO** (enterprise classifier), **OKATO/OKTMO**
  (territory).
- Join keys: **INN** and **OGRN** across GIR BO, RSMP, EGRUL, and the FNS open sets.

## GIR BO (financial statements + identity) — verified

### Search result (organization)
`id`, `inn`, `ogrn`, `shortName`, `index`, `region`, `district`, `city`,
`settlement`, `street`, `house`, `building`, `office`, `okved2`, `okopf`
(`{id,name}`), `okfs` (`{id,name}`), `okpo`, `okato`, `statusCode`, `statusDate`,
`bfo` (list of statement summaries).

### BFO record (per year)
`id`, `period` (year), `knd` (form code, e.g. 0710099), `published`,
`organizationInfo` (`fullName`, `inn`, `kpp`, `okpo`, `okopf`, `okfs`), and the
financial forms — **Бухгалтерский баланс** (balance sheet, form 1) and **Отчёт о
финансовых результатах** (income statement, form 2). Line items use codes like
`current1600` (total assets), `current2110` (revenue), `current2400` (net profit),
with prior-period comparatives. Currency **RUB** (usually thousands).

## RSMP (Unified SME Register) — verified XSD

`ОргВклМСП` (legal entity): `НаимОрг` (full name), `НаимОргСокр` (short name),
`ИННЮЛ` (INN), `ОГРН`. `ИПВклМСП` (individual entrepreneur): `ФИОИП`, `ИННФЛ`,
`ОГРНИП`. `СведМН` (location): `Регион`, `Район`, `Город`, `НаселПункт`,
`КодРегион`. `СвОКВЭД` (activity): `СвОКВЭДОсн` (main, `КодОКВЭД`), `СвОКВЭДДоп`
(additional). `ДатаВклМСП` (date included), `ВидСубМСП` (entity type ЮЛ/ИП),
`КатСубМСП` (category: micro/small/medium), `ПризНовМСП` (newly created),
`ССЧР` (average headcount), licenses, products, social-enterprise flag.

- Format: **XML**, monthly bulk ZIP (chunked XML files), Windows-1251 or UTF-8 per
  the XSD; structure XSD provided.

## EGRUL (authoritative, paid bulk) — fields

OGRN, INN, KPP, full + short name, legal form (OPF), status, registration date,
legal address, directors, founders/participants, charter capital, OKVED, history of
changes. Free per-company extract (выписка, PDF); full bulk = paid FTP.

## Dates, money, encoding

- Dates: `DD.MM.YYYY` (Russian) — normalize to `YYYY-MM-DD`.
- Money: **RUB** (financial statements, usually thousands).
- Encoding: UTF-8 (GIR BO JSON); RSMP/FNS XML may be Windows-1251 — convert.

## Internal model mapping

```text
company_id          <- ОГРН (OGRN, 13-digit)
registration_number <- ОГРН
tax_id              <- ИНН (INN, 10-digit)
vat_id              <- null (VAT/НДС uses the INN; no separate VAT number)
kpp                 <- КПП
legal_name          <- shortName / fullName (НаимОрг)
company_type        <- ОКОПФ (OKOPF legal form)
status              <- statusCode (GIR BO) / EGRUL status
registered_address  <- region/city/street/house (GIR BO) / address (EGRUL)
activity_code       <- ОКВЭД (OKVED)
financials          <- GIR BO BFO (balance sheet + income statement; RUB)
officers            <- EGRUL directors/founders (paid; personal data, 152-ФЗ)
sme_category        <- КатСубМСП (RSMP: micro/small/medium)
employees           <- ССЧР (RSMP headcount) / FNS sshr open set
```
