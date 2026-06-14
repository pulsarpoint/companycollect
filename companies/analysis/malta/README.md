# Company data sources for Malta

## Status

- Official bulk data: **not found** (no open MBR bulk export)
- Official API: **found but paid** (MBR API packages — subscription; free web search is WAF-blocked for bots)
- Open data portal: **found but not the register** (data.gov.mt — WAF-blocked, non-standard; no company bulk)
- License: **unclear** (public register; reuse/redistribution terms not stated)
- Recommended ingestion path: **manual review / per-entity lookup**; the paid MBR API or a commercial provider for bulk + structured financials

## Best source

The authoritative register is the **MBR (Malta Business Registry)**, keyed on the **registration number** (e.g.
`C 12345`; `C` prefix = companies). It is a **public** register, in English:

- **Free** basic company search by name / registration number (basic info + status).
- The register also holds **officers** (directors/secretary), **shareholders** (name, share type, degree of
  control), and **financial information** (annual accounts, annual return) — but those, and certified documents,
  are **paid** (EUR 5–25 per document).
- The MBR has launched official **API packages** (e.g. a Company Search API) — **subscription/paid**.

But the online registry portals (`registry.mbr.mt`, `baros.mbr.mt`) return **HTTP 403** to non-browser clients
(WAF), there is **no open bulk export**, and **data.gov.mt** is WAF-blocked and does not publish the register. So
Malta is a **partial-open / automation-blocked** country: free manual lookups, but lawful automation requires the
**paid MBR API** or a commercial provider.

## Next action

For automation/bulk + structured financials, use the **paid MBR API** packages or a **commercial provider**
(Kyckr, Creditinfo). Validate VAT (`MT`+8 digits) via VIES. Confirm MBR reuse terms before redistribution; treat
officer/shareholder/beneficial-owner data under GDPR (UBO restricted to legitimate interest).
