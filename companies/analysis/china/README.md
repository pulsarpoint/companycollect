# Company data sources for China (CN)

## Status

- Official bulk data: **not found (open)** — GSXT has no open bulk/API.
- Official API: **not found (open)** — GSXT requires real-name authentication + CAPTCHA per query.
- Open data portal: **no national company register** — provincial portals exist but not the register.
- License: **restricted/unclear** — no open re-use terms for the register.
- Recommended ingestion path: **manual review / licensed data** — no lawful open bulk; listed-company financials only.

## Best source

**GSXT — National Enterprise Credit Information Publicity System** (`gsxt.gov.cn`),
run by the State Administration for Market Regulation (**SAMR**) — the
authoritative national register. A per-company search returns the **USCC (Unified
Social Credit Code, 18-character)**, status (存续/在营 = active), legal
representative, registered address, and establishment date. But it is
**Chinese-only**, requires **real-name authentication** (since Nov 2021) **and a
CAPTCHA** on every query, has **no open API or bulk download**, and is frequently
**unreachable externally** (HTTP 521). There is no open national company dataset.

## Financial data

**Open only for listed companies.** Listed-company annual reports and financial
statements are publicly disclosed via the CSRC-designated platform **cninfo
(巨潮资讯网, cninfo.com.cn)** and the **Shanghai/Shenzhen/Beijing stock exchanges**
(A/B shares; H-shares via HKEX). **Non-listed companies do not publicly disclose
financials.** So open structured financials are essentially **listed-only**.

## Next action

For a lawful pipeline: use the GSXT per-company official lookup for verified
identity (respecting real-name + CAPTCHA — no bypass) or a licensed commercial
provider (Qichacha/Tianyancha/Aiqicha, which resell GSXT under their own terms);
pull **listed-company financials** from cninfo/SSE/SZSE. Identity keys on the
**USCC** (= taxpayer id). Legal-representative names are personal data — redact.
