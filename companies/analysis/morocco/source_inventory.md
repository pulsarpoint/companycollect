# Source inventory — Morocco

| Source | Type | Org | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| Casablanca Bourse (`casablanca-bourse.com`) | Listed companies + financials | Bourse de Casablanca | **Open** | html, pdf | yes (listed) | **recommended** |
| OMPIC / directinfo (`directinfo.ma`) | Official register (RCC) | OMPIC | reCAPTCHA search; paid detail + API | html, pdf | yes (Bilans, paid) | blocked_by_payment |
| data.gov.ma (CKAN) | Open-data portal | ADD / MMSP | Public; no company dataset | xlsx/csv/json | no | useful_secondary_source |

## Identifiers

- **ICE — Identifiant Commun de l'Entreprise** — 15-digit **unified** company id
  (registry + tax + social security). Primary join key.
- **RC — Registre du Commerce number** — commercial registration (per court).
- **IF — Identifiant Fiscal** — tax id (DGI).
- **Patente / Taxe Professionnelle**, **CNSS** — local tax / social security numbers.

## Key facts

- **OMPIC** (directinfo.ma) is the official register but its free search is
  **reCAPTCHA-gated** and detailed data + **Bilans** + **API** are **paid**; no open
  bulk.
- **Casablanca Bourse** is the one **open** source — listed companies + financial
  publications (verified live: AFMA, Afric Industries, Alliances, Atlanta Sanad).
- **data.gov.ma** is real CKAN but has **no company register** (statistics only).
- Currency **MAD**; French + Arabic. Dirigeants/associés are personal data (Law
  09-08) → redact.
