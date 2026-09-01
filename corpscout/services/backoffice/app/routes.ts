import {
  type RouteConfig,
  index,
  layout,
  route,
} from "@react-router/dev/routes";

export default [
  layout("routes/shell.tsx", [
    index("routes/home.tsx"),
    route("countries", "routes/countries.tsx"),
    route("countries/:country/companies", "routes/country-companies.tsx"),
    route(
      "countries/:country/domain-suggestions",
      "routes/country-domain-suggestions.tsx",
    ),
    route(
      "countries/:country/address-quality",
      "routes/country-address-quality.tsx",
    ),
    route(
      "countries/:country/contracts/:ref",
      "routes/country-contract-detail.tsx",
    ),
    route(
      "countries/:country/facet-options",
      "routes/country-facet-options.ts",
    ),
    // One level of the CPV tree at a time; see the route for why it is not
    // loader data.
    route(
      "countries/:country/contracts-cpv",
      "routes/country-contracts-cpv.ts",
    ),
    route("countries/:country", "routes/country-layout.tsx", [
      index("routes/country-overview.tsx"),
      route("economy", "routes/country-economy.tsx"),
      route("trade", "routes/country-trade.tsx"),
      route("business", "routes/country-business.tsx"),
      route("contracts", "routes/country-contracts.tsx"),
      route("markets", "routes/country-markets.tsx"),
    ]),
    route("company/:country/:id", "routes/company-layout.tsx", [
      index("routes/country-company-detail.tsx"),
      route("section/:section", "routes/company-section.ts"),
      route("same-address", "routes/company-same-address.ts"),
      route("suggestions", "routes/company-domain-suggestions.tsx"),
      route("financials", "routes/company-financials.tsx"),
      route("technology", "routes/company-technology-layout.tsx", [
        index("routes/company-technology.tsx"),
        route(
          "web-intelligence",
          "routes/company-technology-web-intelligence.tsx",
        ),
        route("infrastructure", "routes/company-technology-infrastructure.tsx"),
        route("ip-addresses", "routes/company-technology-ip-addresses.tsx"),
        route(
          "ip-addresses/:address",
          "routes/company-technology-ip-address.tsx",
        ),
      ]),
      route(
        "financials/esef/:documentId",
        "routes/company-esef-financial-report.tsx",
      ),
      route(
        "financials/esef/:documentId/notes",
        "routes/company-esef-report-notes.tsx",
      ),
      route("financials/:documentId", "routes/company-financial-report.tsx"),
    ]),
    route("company/:country/:id/facts/:year", "routes/company-facts.tsx"),
    route(
      "company/:country/:id/facts/:year/document",
      "routes/company-facts-document.ts",
    ),
    route("company/:country/geocode", "routes/country-geocode.ts"),
    // Deliberately not nested under a country: a source page shows the whole
    // register, including the winners no country view will ever carry.
    route("procurements", "routes/procurements.tsx"),
    route("procurements/:source", "routes/procurement-source.tsx"),
    route("procurements/:source/:key", "routes/procurement-record.tsx"),
    route("ip/:address", "routes/ip-address.tsx"),
    route("financial-demo", "routes/financial-demo.tsx"),
    // Old bookmarks: /financials/country/:c still lands on the country page.
    route("financials/country/:country", "routes/financials-country.tsx"),
  ]),
  // Not a page: streams technology-catalog icons from the object store. The
  // browser only ever sees this URL — bucket and credentials stay server-side.
  route("icons/tech/:slug", "routes/tech-icon.ts"),
  // Codex-thread resource APIs (JSON; no admin chrome). The demo subpage uses
  // the same service functions through its own actions.
  route("admin/api/codex/threads", "routes/admin-api-codex-threads.ts"),
  route("admin/api/codex/threads/:threadId", "routes/admin-api-codex-thread.ts"),
  route(
    "admin/api/codex/threads/:threadId/messages",
    "routes/admin-api-codex-thread-messages.ts",
  ),
  route("admin", "routes/admin-layout.tsx", [
    index("routes/admin-index.tsx"),
    route("esef", "routes/admin-esef.tsx"),
    // The technology catalog browser and its per-technology detail page.
    // Country-agnostic (the catalog is global), so they sit beside the other
    // workspace pages, not under a country prefix.
    route("technologies", "routes/admin-technologies.tsx"),
    route("technologies/:slug", "routes/admin-technology-detail.tsx"),
    route("general/roles", "routes/admin-general-roles.tsx"),
    route("settings/llms", "routes/admin-settings-llms.tsx"),
    route("settings/llms/local", "routes/admin-settings-llms-local.tsx"),
    route("se/people", "routes/admin-se-people.tsx"),
    route(
      "se/people/person/:companyId/:personId",
      "routes/admin-se-people-person.tsx",
    ),
    // Backoffice-triggered runs for the ClickHouse company-person model
    // (identity evaluation, resolution, merge suggestions) -- SE People
    // Experiment Task 5, mirroring se/companies/pipeline's confirm-then-launch
    // pattern. A real page, not a sheet: see the route's own docstring.
    route("se/people/pipeline", "routes/admin-se-people-pipeline.tsx"),
    // One company, nine tabs. The layout owns the header and the sub-menu; a
    // bare /admin/se/company/:companyId redirects to Info.
    route("se/company/:companyId", "routes/admin-se-company-layout.tsx", [
      index("routes/admin-se-company-index.tsx"),
      route("info", "routes/admin-se-company-info.tsx"),
      route("address", "routes/admin-se-company-address.tsx"),
      route("financial", "routes/admin-se-company-financial.tsx"),
      // ESEF is an area: Info (aggregated extraction) as index, then one
      // sub-tab per filed document with facts, notes, and LLM subpages.
      route("esef", "routes/admin-se-company-esef-layout.tsx", [
        index("routes/admin-se-company-esef.tsx"),
        route(":documentId", "routes/admin-se-company-esef-document.tsx"),
        route(
          ":documentId/notes",
          "routes/admin-se-company-esef-notes.tsx",
        ),
        route(":documentId/llm", "routes/admin-se-company-esef-llm.tsx"),
      ]),
      route("people", "routes/admin-se-company-people.tsx"),
      route("domains", "routes/admin-se-company-domains.tsx"),
      // The whole public technology area, inside the admin panel: the same
      // sub-tabs as /company/:country/:id/technology, nested the same way,
      // on the admin base path and without the public 404-on-empty.
      route("technology", "routes/admin-se-company-technology-layout.tsx", [
        index("routes/admin-se-company-technology.tsx"),
        route(
          "web-intelligence",
          "routes/admin-se-company-technology-web-intelligence.tsx",
        ),
        route(
          "infrastructure",
          "routes/admin-se-company-technology-infrastructure.tsx",
        ),
        route(
          "ip-addresses",
          "routes/admin-se-company-technology-ip-addresses.tsx",
        ),
        route(
          "ip-addresses/:address",
          "routes/admin-se-company-technology-ip-address.tsx",
        ),
        route(
          "mail-security",
          "routes/admin-se-company-technology-mail-security.tsx",
        ),
      ]),
      route("contracts", "routes/admin-se-company-contracts.tsx"),
      route("jobs", "routes/admin-se-company-jobs.tsx"),
      route("listed", "routes/admin-se-company-listed.tsx"),
    ]),
    // The all-companies LIST area: one tabbed section. The layout owns the
    // header and tab bar; Info is the default (index). Sibling of the
    // single-company DETAIL area above, and a different thing (many companies).
    route("se/companies", "routes/admin-se-companies-layout.tsx", [
      index("routes/admin-se-companies-info.tsx"),
      route("geocoding", "routes/admin-se-companies-geocoding.tsx"),
      route("financial", "routes/admin-se-companies-financial.tsx"),
      route("ratsit", "routes/admin-se-companies-ratsit.tsx"),
    ]),
    // Not a page: the resource route behind the companies list's Pipeline
    // sheet. Its loader answers that sheet's fetcher and redirects anyone who
    // navigates to the URL back to the list.
    route("se/companies/pipeline", "routes/admin-se-companies-pipeline.ts"),
    // The correction ledger is not a tab: it keeps its own URL and is reached
    // as a secondary link from the Info tab.
    route(
      "se/company-info/corrections",
      "routes/admin-se-company-info-corrections.tsx",
    ),
    // Old bookmarks: the list and the geocoding view moved under se/companies.
    // Thin loader-only routes that 302 to the new URLs.
    route("se/company-info", "routes/admin-se-company-info-redirect.ts"),
    route(
      "se/company-info/geocoding",
      "routes/admin-se-company-info-geocoding-redirect.ts",
    ),
    route(
      "se/company-address/corrections",
      "routes/admin-se-company-address-corrections.tsx",
    ),
    route(
      "se/people/stale-corrections",
      "routes/admin-se-people-stale-corrections.tsx",
    ),
  ]),
] satisfies RouteConfig;
