# KRS API — Field Catalog

## Source Summary

- Country: Poland
- Source type: official_registry_api (the open spine)
- Organization: Ministerstwo Sprawiedliwości (Ministry of Justice)
- URL: `https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs}?rejestr={P|S}&format=json` (also OdpisPelny)
- License: open / free reuse; **personal data anonymized** (GDPR)
- Access: public, **no auth**
- Freshness: authoritative / continuous
- Record shape: `{ odpis: { rodzaj, naglowekA, dane: { dzial1..6 } } }`
- Primary keys: `naglowekA.numerKRS`
- Join keys: `numerKRS`, `identyfikatory.nip`, `identyfikatory.regon`

## Fields

| Path | Source field (PL) | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| odpis.rodzaj | rodzaj | Aktualny/Pelny | string | metadata | `Aktualny` | current vs full |
| naglowekA.numerKRS | numerKRS | KRS number (10) | string | identifier | `0000026438` | **PK** |
| naglowekA.rejestr | rejestr | P/S | string | metadata | `P` | companies/NGOs |
| naglowekA.dataRejestracjiWKRS | dataRejestracjiWKRS | Registration date | date | date | — | incorporation |
| dzial1.danePodmiotu.nazwa | nazwa | Legal name | string | legal_name | `POWSZECHNA KASA…` | |
| dzial1.danePodmiotu.formaPrawna | formaPrawna | Legal form | string | legal_form | `SPÓŁKA AKCYJNA` | |
| …identyfikatory.nip | nip | Tax id (10) | string | identifier | `5250007738` | VAT = PL+NIP |
| …identyfikatory.regon | regon | REGON | string | identifier | `01629826300000` | 14-digit; normalize to 9 |
| dzial1.siedzibaIAdres.siedziba | siedziba | Seat (woj/powiat/gmina/miejscowosc) | object | geography | `MAZOWIECKIE…WARSZAWA` | region/municipality |
| dzial1.siedzibaIAdres.adres | adres | Registered address | object | address | `UL. ŚWIĘTOKRZYSKA 36…` | concatenate |
| …adresStronyInternetowej | website | Website | string | metadata | — | discovery |
| dzial1.kapital.wysokoscKapitaluZakladowego | kapitał zakładowy | Share capital | object | financial | `1250000000,00 PLN` | register capital |
| dzial2.reprezentacja | reprezentacja | Board (zarząd) | object | person | — | **anonymized** |
| dzial2.organNadzoru | organNadzoru | Supervisory board | object | person | — | anonymized |
| dzial3.…przedmiotPrzewazajacejDzialalnosci | PKD | Primary activity | array | activity | `64.19.Z POZOSTAŁE POŚREDNICTWO…` | PKD 2007 |
| dzial3.wzmiankiOZlozonychDokumentach | wzmianki | Filed-document mentions | object | filing | annual statement/auditor/approval | **→ trigger RDF** |
| dzial3.informacjaODniuKonczacymRokObrotowy | rok obrotowy | Fiscal year end | object | date | `31.12` | financial alignment |
| dzial6 | dzial6 | Liquidation/bankruptcy/merger | object | status | `PRZEJĘCIE INNEJ SPÓŁKI` | status signal |

## Interpretation Notes

- **The open spine** — free, no-auth JSON with the full current/full extract; everything joins on
  **numerKRS** (and the embedded **NIP/REGON**). Personal data is **anonymized** by the API (board members
  appear without natural-person identifiers) — GDPR-friendly.
- **Registers**: `P` (companies) and `S` (NGOs). Entities are in one register; sole proprietors are **not
  here** (CEIDG).
- **Financial linkage**: `dzial3.wzmiankiOZlozonychDokumentach` lists which annual statements were filed
  (with periods) → use as the trigger to fetch the actual figures from the **RDF** (`krs_rdf_financials`).
- **Codes**: PKD activity (assemble `kodDzial.kodKlasa.kodPodklasa`, e.g. `64.19.Z`); Polish decimals use a
  comma (`1250000000,00`).
- **Status**: derive from `dzial6` (likwidacja/upadłość) + the white list `statusVat`.
- See `sample_record.json` for a real trimmed record (PKO BP, KRS 0000026438).
