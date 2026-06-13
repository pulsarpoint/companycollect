# Norway Company Profile — Mapping Report

This maps each section of `country_company_profile.schema.json` to its source field path,
source slug, join key, freshness, and precedence rules.

## Sources & precedence

| Slug | Source | Role | Join key | Freshness | License/access |
|---|---|---|---|---|---|
| brregenhet | Enhetsregisteret entities | **Base record** (spine) | organisasjonsnummer | daily | NLOD 2.0, public |
| brregunderenhet | Enhetsregisteret sub-entities | Establishments/sites | overordnetEnhet → enhet | daily | NLOD 2.0, public |
| brregroller | Enhetsregisteret roles | Officers (PII) | {orgnr} → enhet | daily | NLOD 2.0, public (GDPR) |
| brregregnskap | Regnskapsregisteret | Financial statements | virksomhet.organisasjonsnummer → enhet | weekly | NLOD 2.0, public (open API "temporary/research") |

**Precedence**: all four are the same official publisher (Brreg), so there are no
cross-source conflicts to resolve — they are complementary, joined on `organisasjonsnummer`.
`brregenhet` is the spine; the others attach as arrays/sub-objects. When both
Regnskapsregisteret SELSKAP and KONSERN accounts exist for a year, prefer SELSKAP for the
entity's own figures and keep KONSERN as the consolidated view.

## Field mapping

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.organisasjonsnummer | brregenhet | organisasjonsnummer | self (PK) | only source | string; mod-11 |
| registration.vat_id | brregenhet | registrertIMvaregisteret (derived) | — | derived | `NO`+orgnr+`MVA` when true |
| registration.registered_in_ccr_date | brregenhet | registreringsdatoEnhetsregisteret | — | only source | |
| registration.register_memberships.* | brregenhet | registrertI{Foretaks,Mva,Stiftelses,Frivillighets,Parti}registeret | — | only source | booleans |
| legal_identity.navn | brregenhet | navn | — | only source | current name only |
| legal_identity.legal_form.* | brregenhet | organisasjonsform.{kode,beskrivelse} | — | only source | |
| legal_identity.language_form | brregenhet | maalform | — | only source | |
| legal_identity.share_capital.* | brregenhet | kapital.{belop,valuta,antallAksjer} | — | only source | AS/ASA; entity API only |
| status.derived | brregenhet | konkurs/underAvvikling/underTvangsavvikling... | — | derived | see rule below |
| status.* (flags) | brregenhet | konkurs, underAvvikling, underTvangsavviklingEllerTvangsopplosning | — | only source | dates in bulk CSV |
| activity.naeringskoder[] | brregenhet | naeringskode1..3 | — | only source | SN2007/NACE |
| activity.institusjonell_sektorkode | brregenhet | institusjonellSektorkode | — | only source | |
| activity.vedtektsfestet_formaal | brregenhet | vedtektsfestetFormaal | — | only source | array of lines |
| activity.aktivitet_tekst | brregenhet | aktivitet | — | only source | array of lines |
| addresses.forretningsadresse | brregenhet | forretningsadresse | — | only source | business/visiting |
| addresses.postadresse | brregenhet | postadresse | — | only source | mailing |
| contact.hjemmeside | brregenhet | hjemmeside | — | only source | unvalidated |
| contact.telefon | brregenhet | telefon | — | only source | |
| contact.epostadresse | brregenhet | epostadresse | — | only source | bulk CSV only; possible PII |
| employment.antall_ansatte | brregenhet | antallAnsatte | — | only source | when har... true |
| group.er_i_konsern | brregenhet | erIKonsern | — | only source | members not enumerated |
| group.overordnet_enhet | brregenhet | overordnetEnhet | → enhet | only source | parent org number |
| establishments[] | brregunderenhet | (sub-entity record) | overordnetEnhet = registration.organisasjonsnummer | only source | site-level |
| establishments[].beliggenhetsadresse | brregunderenhet | beliggenhetsadresse | — | only source | physical site |
| officers[] | brregroller | rollegrupper[].roller[] | {orgnr} = registration.organisasjonsnummer | only source | **PII/GDPR** |
| officers[].person.fodselsaar | brregroller | person.fodselsdato (minimized) | — | derived | birth year only |
| officers[].enhet.organisasjonsnummer | brregroller | enhet.organisasjonsnummer | → enhet | only source | corporate holder (auditor) |
| financial_statements[] | brregregnskap | (array element) | virksomhet.organisasjonsnummer = registration.organisasjonsnummer | only source | per period × type |
| financial_statements[].valuta | brregregnskap | valuta | — | only source | **required; not always NOK** |
| financial_statements[].revenue | brregregnskap | ...driftsinntekter.sumDriftsinntekter | — | only source | |
| financial_statements[].net_result | brregregnskap | resultatregnskapResultat.aarsresultat | — | only source | |
| financial_statements[].total_assets | brregregnskap | eiendeler.sumEiendeler | — | only source | |
| financial_statements[].total_equity | brregregnskap | egenkapitalGjeld.egenkapital.sumEgenkapital | — | only source | |
| financial_statements[].total_debt | brregregnskap | egenkapitalGjeld.gjeldOversikt.sumGjeld | — | only source | |
| filing_signals.siste_innsendte_aarsregnskap | brregenhet | sisteInnsendteAarsregnskap | — | only source | refresh trigger for financials |

## Status derivation rule

```
if konkurs                                     -> "bankrupt"
elif underTvangsavviklingEllerTvangsopplosning -> "compulsory_liquidation"
elif underAvvikling                            -> "liquidation"
else                                           -> "active"
```

## Join order (build sequence)

1. Load `brregenhet` as the spine (org number → profile).
2. Attach `brregunderenhet` establishments where `overordnetEnhet == organisasjonsnummer`.
3. Attach `brregroller` officers per org number (apply PII minimization).
4. Attach `brregregnskap` financial_statements per org number (gate fetch on
   `sisteInnsendteAarsregnskap`).

## Missing-data notes

- **Beneficial ownership** (reelle rettighetshavere): not in open data → not in profile.
- **National ID (fødselsnummer)** for officers: only via authenticated autorisert-api → excluded.
- **Multi-year financial history**: open API depth is shallow; accumulate snapshots or use the
  paid Subscription Service.
- **Group membership detail**: only an `erIKonsern` boolean + `overordnetEnhet`; full group tree
  is not enumerated by these sources.
