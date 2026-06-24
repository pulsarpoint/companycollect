# Iceland Company Data Investigation

## Conclusion

Iceland has a **free per-company company register lookup** but **no open bulk**, and
**no open financial bulk** (filed annual accounts are paid per-document):

- **Identity (free per-company):** **Fyrirtækjaskrá**, the national company register
  run by **Skatturinn**. Keyed on the **kennitala** (10-digit). The free overview
  (`gjaldfrjálst yfirlit`) exposes name, kennitala, registered address (lögheimili),
  municipality (sveitarfélag), legal form (rekstrarform), ÍSAT activity, VSK status,
  and the chair (forráðamaður — personal data). Bulk extracts + certified
  certificates are **paid** (a fee schedule / gjaldskrá applies).
- **Financials (paid):** the **Ársreikningaskrá** (Annual Accounts Register,
  Skatturinn) receives electronically filed annual accounts for **public
  disclosure**, but retrieval is paid per-document — no open bulk/XBRL.
- **VAT (VSK):** Iceland has VAT; the VSK number is a separate registration shown in
  the register.

## What was verified live

- **Fyrirtækjaskrá per-company pages work**: real records at
  `…/fyrirtaekjaskra/leit/kennitala/{kennitala}` — e.g. `6204830369` **JBT Marel
  ehf.** (Austurhrauni 9, 210 Garðabær; rekstrarform `E1 Einkahlutafélag (ehf)`),
  `4612023490` **Icelandair ehf.** (Flugvöllum 1, 221 Hafnarfjörður), `6306261610`
  a húsfélag (housing association, ÍSAT `94.99.1`).
- The register page offers a free overview plus paid **Staðfest vottorð**
  (certificate of registration), and a **gjaldskrá** (fee schedule) page confirms
  bulk/extract fees.
- **Ársreikningaskrá** page confirms electronic filing (Hnappurinn) "til
  opinberrar birtingar" (for public disclosure).
- **opingogn.is** redirects to island.is; the legacy CKAN API is gone; the register
  is not openly hosted there.

## Identifiers

- **kennitala** — 10-digit national identifier for **both** legal entities and
  individuals; for companies it is the **company id and the tax id**. Company
  kennitalas add **40** to the day field (first two digits 41–71), distinguishing
  them from individuals' kennitalas.
- **VSK-númer** (VAT) — Iceland has **VAT (VSK)**; the VAT registration number is
  **separate** from the kennitala. The register shows VSK status.

## Register fields (free overview, verified)

`nafn` (name), `kennitala`, `lögheimili` (registered address), `sveitarfélag`
(municipality, with code), `rekstrarform` (legal form, e.g. `E1 Einkahlutafélag
(ehf)` private ltd; `Hlutafélag (hf)` public ltd; sf/slf partnerships; svf co-op;
sjálfseignarstofnun foundation), `ÍSAT atvinnugrein` (economic activity, NACE-based),
`VSK-skrá` (VAT-register status), `forráðamaður` (responsible person / chair —
**personal data**).

## What is NOT openly available

- **Bulk company register** — paid extracts (gjaldskrá); free access is per-company.
- **Financial statements (bulk)** — paid per-document (Ársreikningaskrá).
- **Certified certificates** (Staðfest vottorð) — paid.
- The kennitala/VSK are exposed per-company but there is no open dataset/API.

## Recommended ingestion

1. **Fyrirtækjaskrá** per-company overview (free), keyed on kennitala, for identity
   (name, address, legal form, ÍSAT, VSK status).
2. Treat bulk extracts, certified certificates, and the **Annual Accounts Register**
   (financials) as **paid**.
3. Redact the chair/forráðamaður (personal data, GDPR) in shared samples.
