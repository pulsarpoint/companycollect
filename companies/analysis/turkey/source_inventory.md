# Turkey — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| MERSIS | Ministry of Trade | official registry | free per-company; no open bulk | HTML | free/no-bulk | **recommended** (per-company) |
| Ticaret Sicil Gazetesi | TOBB | official gazette | public search | HTML, PDF | public gazette | useful_secondary_source |
| KAP | MKK / Borsa İstanbul | financial disclosure | public (listed) | HTML, PDF, XBRL | public disclosure | **recommended** (listed) |
| GİB (VKN) | Revenue Administration | tax registry | per-company lookup | HTML | free | useful_secondary_source |

## Roles

- **mersis_registry** — authoritative identity (MERSIS no, title, VKN, NACE,
  address, type, status); free per-company query, no open bulk.
- **ticaret_sicil_gazetesi** — company events (registration/changes/dissolution).
- **kap_disclosure** — listed-company financial statements + disclosures. Verified
  live (808 listed companies extracted).
- **gib_vkn** — VKN / KDV taxpayer lookup.

## Join keys

**MERSIS no** (16-digit) and **VKN** (10-digit) across the registry/gazette/tax
sources; **KAP id**/name for listed financials. The VKN is the tax id; Turkey has
VAT (KDV) with no separate VAT number.
