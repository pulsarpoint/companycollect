# ONRC Beneficial Ownership Register (RBR) Field Catalog

> **PLANNING-ONLY / RESTRICTED.** Cataloged from public ONRC documentation only.
> Access requires online registration, an administrative **fee**, and a
> **qualified electronic signature**; narrowed to **legitimate interest** after
> CJEU C-37/20. No raw records, values, or sample retrieved. PERSONAL DATA.

## Source Summary

- Country: Romania
- Source type: beneficial_ownership
- Organization: ONRC (*Registrul Beneficiarilor Reali*)
- URL: https://www.onrc.ro/index.php/ro/informatii-privind-beneficiarii-reali
- License: restricted
- Access: restricted (registration + fee + qualified e-signature; legitimate interest)
- Freshness: unknown
- Record shape: planning-only
- Primary keys: `cui`
- Join keys: `cui`, `cod_inmatriculare`

## Fields (from public docs)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| beneficiari[].nume | nume/prenume | Beneficial owner name | string | ownership | planning-only; PII |
| beneficiari[].modalitate_control | modalitate de control | Nature of control | string | ownership | planning-only |
| beneficiari[].cetatenie | cetatenie | Citizenship | string | ownership | planning-only; PII |

## Interpretation Notes

- The only authoritative source of **beneficial owners** in Romania, but **not
  open**: gated by registration, fee, and qualified e-signature, with access
  restricted to legitimate interest. Keep entirely **planning-only**.
- Officers (legal representatives) are available openly via
  `OD_REPREZENTANTI_LEGALI`, but **beneficial ownership / shareholders** are not.
