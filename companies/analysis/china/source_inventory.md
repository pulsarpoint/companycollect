# China — Source Inventory

| Source | Slug | Type | Access | License | Format | Status |
|---|---|---|---|---|---|---|
| GSXT national register | gsxt_search | official_registry | real-name + CAPTCHA | restricted/unclear | html | blocked_by_authentication |
| cninfo / SSE / SZSE financials | cninfo_disclosure | official_financial | public (per-issuer) | issuer disclosure | pdf | useful_secondary_source |
| Credit China | credit_china | official_registry | public (bot-protected) | restricted/unclear | html | useful_secondary_source |
| Aggregators (Qichacha/Tianyancha/Aiqicha) | cn_aggregators | aggregator | search/paid | vendor terms | html/json | blocked_by_license_uncertainty |

## Best (constrained) path

There is **no lawful open bulk** for Chinese companies. The authoritative **GSXT**
register is real-name + CAPTCHA gated (no open API/bulk); full coverage realistically
needs a **licensed commercial provider**. **Financials are listed-only**
(cninfo/SSE/SZSE). Everything keys on the **USCC (18-char)** = taxpayer id.

## Downloaded

- (none — GSXT gated/unreachable; financials per-issuer view-only; aggregators anti-bot)
- `normalized/companies.sample.jsonl` — **schematic** record (no open per-company
  bulk lawfully downloadable; register is gated)
