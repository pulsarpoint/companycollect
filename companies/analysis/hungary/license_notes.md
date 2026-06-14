# Hungary — License & Terms Notes

> Hungarian financial statements are free to view and basic company identity is free to search, but there is no
> stated open-data licence, no bulk export, the financials search is reCAPTCHA-gated, and full register data is
> paid.

## e-beszámoló (Electronic Financial Reports Portal)
- All annual financial statements are **public and free to view** with **no registration**. However:
  - The search endpoint (`/Search/Results`) is **reCAPTCHA-protected** (verified: `{"errorText":"A reCaptcha
    kitöltése nem megfelelő."}`). Automated/bulk scraping is an **access-control bypass** — **do not** do it.
  - No open-data **licence** for reuse/redistribution is stated — confirm terms before redistribution. Public
    viewing ≠ permission to redistribute.

## e-cégjegyzék / Cégszolgálat
- **Free basic** company information ("Ingyenes Céginformáció"). **Full/certified** extracts (cégkivonat,
  officers, owners, history) are **paid** via the Céginformációs Szolgálat or commercial resellers.

## NAV áfaalanyok (VAT subjects)
- NAV taxpayer/VAT-subject databases are **public** (közadat), updated daily; single + batch query, some CSV
  downloads. Use for tax-number/VAT validation; follow NAV's terms for any reuse.

## VIES
- Validates a supplied HU EU VAT number. Validation/enrichment only; not redistributable as a list.

## KSH
- Statistical business register / aggregate statistics + the statisztikai számjel and TEÁOR classification.
  Open under KSH terms; not a per-company open master.

## EKR / procurement
- Open procurement data referencing supplier **adószám + name**; reusable under its terms as a cross-reference.

## Commercial aggregators
- OPTEN, Bisnode, Céginfo, companyapi.hu, etc. resell the full cégjegyzék + parsed financials under
  **commercial, per-vendor contracts**.

## Personal data / GDPR
- Officers/representatives and any natural-person data are **personal data** — apply a GDPR lawful basis +
  retention before persisting; no direct-marketing reuse.

## Summary recommendation
- **Free (manual only)**: viewing e-beszámoló financials + e-cégjegyzék basic info.
- **Blocked for automation**: e-beszámoló search (reCAPTCHA) — do not bypass.
- **Paid**: full register data; structured financials at scale via a vendor.
- **Validation**: NAV áfaalany + VIES.
- Confirm reuse terms; treat officer data under GDPR.
