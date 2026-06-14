# Austria — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **Firmenbuch** (JustizOnline / Verrechnungsstellen) | Official registry | Free brief / paid full / ID-gated API | HTML/PDF/JSON | Contractual | blocked by payment (authoritative) |
| **Jahresabschluss** (Urkundensammlung) | Official financials | Paid per document | PDF / filing | Contractual | blocked by payment (**financials**) |
| **GISA — Gewerbe in Österreich** | Open trade authorizations | Free public | CSV/JSON | Open (data.gv.at) | **recommended** (open **subset**) |
| Ediktsdatei / Insolvenzdatei | Insolvency gazette | Free web / licensed feed | HTML/JSON | Free; feed needs IWG licence | useful secondary (insolvency signal) |
| WiEReG (beneficial ownership) | BO register | Restricted/fee | PDF | Restricted | blocked by authentication |
| data.gv.at | Open data portal | Free | CSV/JSON/XML | Per dataset | useful secondary (discovery) |
| Commercial aggregators (Compass, KSV1870, firmafind, D&B) | Commercial API | Paid | JSON/PDF | Commercial | blocked by payment (financials at scale) |

## Access points

- Firmenbuch (free brief): https://justizonline.gv.at/jop/web/firmenbuchabfrage — full/docs via Verrechnungsstellen (Compass, KSV1870, HF data, Lexunited, Manz)
- GISA open data: https://www.data.gv.at/katalog/dataset/gewerbe-in-osterreich (GISA CSV + JSON); free query https://www.gisa.gv.at/abfrage
- Insolvency gazette: https://edikte.justiz.gv.at/ (free web); structured feed https://iwg.justiz.gv.at/edikte/ (IWG licence)
- Beneficial ownership (restricted): WiEReG (BMF)
- National catalog: https://www.data.gv.at/
- Aggregators: https://firmafind.at/ , Compass, KSV1870

## Key facts

- **No open per-company master, no open bulk financials** — Firmenbuch + Jahresabschluss are **paid** (clearing houses).
- **Open** = GISA trade authorizations (data.gv.at) + free brief Firmenbuch extract + free insolvency-gazette queries.
- **JustizOnline API** is free but **ID-Austria-gated**; **insolvency structured feed** needs an **IWG licence**.
- Identifiers: **Firmenbuchnummer** (FN + check letter), **UID** (ATU########), **GISA-Zahl**.

See `source_inventory.json` for the machine-readable version.
