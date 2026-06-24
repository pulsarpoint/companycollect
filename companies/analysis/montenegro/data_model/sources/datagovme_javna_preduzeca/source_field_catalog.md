# data.gov.me — Javna preduzeća (public enterprises) Field Catalog

## Source Summary

- Country: Montenegro
- Source type: company_list (public/state enterprises)
- Organization: Ministarstvo javne uprave via data.gov.me
- URL: https://data.gov.me/dataset/13740b69-cddf-4f35-966d-52df7a9661ce
- License: openly published (data.gov.me)
- Access: public (XLSX + CKAN API)
- Freshness: periodic
- Record shape: XLSX, one row per public enterprise
- Primary keys: Naziv (name)
- Join keys: Naziv

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Naziv | Naziv | Name | string | legal_name | Luka Bar AD | public enterprises only |
| Status | Status | Status | string | status | Aktivna |  |
| Tip | Tip | Entity type | string | legal_form | Javno preduzeće |  |
| Osnivac | Osnivač | Founder | string | ownership | Vlada CG | the state (not PII) |
| Adresa | Adresa | Address | string | address | Podgorica |  |
| Website | Website | Website | string | raw_extension | https://www.irfcg.me/ |  |
| PravniOsnov | Pravni osnov | Legal basis | string | document | Zakon o IRF… |  |

## Interpretation Notes

- A **working, openly-licensed** CKAN dataset listing Montenegro's **public/state
  enterprises** (Ministry of Public Administration). Verified real entities:
  **Investiciono-razvojni fond Crne Gore A.D.**, **Crnogorski elektroprenosni
  sistem AD**, **Luka Bar AD**, **Pošta Crne Gore AD**, **Plantaže AD**, **Rudnik
  uglja AD Pljevlja**, **Crnogorska plovidba AD Kotor**, **MONTECARGO AD**,
  **Željeznički prevoz AD Podgorica**.
- **Scope limit**: this is **only public enterprises**, not the full register, and
  it carries **no PIB / registration number** — those are held by CRPS. So it is a
  **useful secondary** anchor (names/status/type/founder/address/website), not a
  replacement for the register.
- Founders here are the **state** (legal entity), so not personal data.
- Downloaded as XLSX; safe-sampled via sharedStrings (no full parse).
