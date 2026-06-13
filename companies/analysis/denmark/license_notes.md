# Denmark — license & terms notes

## Summary

CVR (Det Centrale Virksomhedsregister) data is **public and free to reuse, including for
commercial purposes**, under Danish law (CVR-loven — Lov om Det Centrale Virksomhedsregister).
Erhvervsstyrelsen distributes it at no charge via `distribution.virk.dk`. No payment is required
for either the base register or the financial statements.

## Base register (cvr-permanent)

- **Access:** free, but requires HTTP Basic credentials obtained by emailing
  `cvrselvbetjening@erst.dk`. As part of access you sign a **declaration** committing to comply
  with the rules on **protected persons / address protection** and data-protection conditions.
- **Reuse:** permitted, including commercially. CVR basic data is explicitly intended for reuse.

## Financial statements (offentliggoerelser / regnskaber)

- **Access:** completely open, no credentials. Documents (XBRL/iXBRL/PDF) downloadable freely.
- **Reuse:** same free-reuse basis; published annual reports are public records.

## Key obligation: Reklamebeskyttelse (advertising protection)

- Companies/units can register **reklamebeskyttelse** in CVR. Their basic data **may not be used
  by private parties for direct marketing**.
- When redistributing CVR data, advertising-protected entities **must be clearly flagged**.
- Erhvervsstyrelsen can **restrict access** for parties that violate these rules.
- Practical implication: carry the protection flag through to the internal model and gate any
  marketing/outreach use on it.

## GDPR / personal data

- `deltager` (participants) and beneficial-ownership (*reelle ejere*) data include natural
  persons. This is public via CVR but still personal data — handle under GDPR, respect address
  protection, and avoid uses incompatible with the register's purpose.

## Uncertainty / to confirm

- Exact wording of the signed declaration and any attribution requirement is established at the
  point of requesting credentials — confirm with Erhvervsstyrelsen during onboarding.
- Recommended attribution string: **"Kilde: CVR / Erhvervsstyrelsen"**.

## References

- CVR-loven (Lov om Det Centrale Virksomhedsregister): https://www.retsinformation.dk/eli/lta/2019/1052
- Erhvervsstyrelsen — CVR samler og udstiller data: https://erhvervsstyrelsen.dk/cvr-samler-og-udstiller-data
- System-til-system adgang (catalog): http://datahub.virk.dk/dataset/system-til-system-adgang-til-cvr-data
