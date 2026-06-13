# Depósito de Cuentas Anuales (Registro Mercantil) — XBRL — Field Catalog

> **PLANNING-ONLY (paid).** Annual accounts for the **general (non-listed) company population**, in
> **XBRL** (Spanish PGC taxonomy) + PDF. Retrieved **per company for ~€8.99–€20** via registradores.org;
> **no bulk, no free API**; retained 6 years. Cataloged from public documentation + the PGC/XBRL taxonomy
> structure — **no records or values copied**.

## Source Summary

- Country: Spain
- Source type: official_financial_disclosure
- Organization: Colegio de Registradores de España (CORPME)
- URL: https://sede.registradores.org/site/mercantil
- License: per-document fee; reuse restricted → planning-only
- Access: **paid** (per company), no registration required, search by NIF/name
- Freshness: annual; retained 6 years
- Record shape: per-company **ZIP** — accounts in **XBRL/XML** (individual + consolidated) + PDF (audit,
  memoria, informe de gestión) + sector-position table
- Primary keys: NIF/CIF + ejercicio
- Join keys: NIF/CIF; denominación + provincia; hoja registral

## Fields

| Path | Source field (ES) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| deposito.nif | NIF/CIF | Company tax id | string | identifier | join key |
| deposito.ejercicio | ejercicio | Fiscal year | integer | date | ≤6 yrs |
| deposito.tipo | tipo de cuentas | normal/abreviado/PYME/micro; ind/consol | string | filing | drives nullability |
| balance.total_activo | Total activo | Total assets | decimal | financial | ~always present |
| balance.activo_no_corriente | Activo no corriente | Non-current assets | decimal | financial | |
| balance.activo_corriente | Activo corriente | Current assets | decimal | financial | |
| balance.patrimonio_neto | Patrimonio neto | Equity | decimal | financial | |
| balance.pasivo | Pasivo (NC + C) | Liabilities | decimal | financial | |
| pyg.cifra_negocios | Importe neto de la cifra de negocios | Revenue | decimal | financial | reduced for micro |
| pyg.resultado_ejercicio | Resultado del ejercicio | Net result | decimal | financial | neg = loss |
| empleados.numero_medio | Número medio de empleados | Avg employees | integer | employment | in memoria |
| documentos | auditoría/memoria/informe gestión | Component docs | array | document | PDF |
| posicion_sector | posición económico-financiera por sector | Sector ratios | object | financial | benchmark |

## Interpretation Notes

- **This is the financial source for the ~3.3M general company population** — the open BORME/OpenMercantil
  data carries no accounts. But it is **paid per company (~€9–20)** with **no bulk and no free API**, so
  scale means many paid lookups or a commercial aggregator (eInforma/Informa D&B, Axesor).
- **Size model drives disclosure.** `tipo de cuentas` (PGC **normal / abreviado / PYME / microempresa**)
  governs how much is filed — the smallest models reduce/omit P&L detail, so `revenue`/`net_result`/
  `employees` are **nullable**. Capture individual vs **consolidado**.
- **Format.** Accounts are **XBRL/XML** (parse with Arelle against the ES PGC taxonomy); narrative docs
  (audit/memoria/informe de gestión) are **PDF**. Numbers are es-locale.
- **Matching.** Best join is **NIF/CIF**; since open data has sparse CIF, fall back to denominación +
  provincia or the hoja registral.
- **No `sample_record.json`** — paid source; values not retrievable under planning-only terms.
