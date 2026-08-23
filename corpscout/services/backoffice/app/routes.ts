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
    route("people", "routes/people.tsx"),
    route("ip/:address", "routes/ip-address.tsx"),
    route("financial-demo", "routes/financial-demo.tsx"),
    route(
      "country/:country/person-targets",
      "routes/country-person-targets.ts",
    ),
    route("country/:country/person/:id", "routes/country-person.tsx"),
    route("person/:name", "routes/person.tsx"),
    // Old bookmarks: /financials/country/:c still lands on the country page.
    route("financials/country/:country", "routes/financials-country.tsx"),
  ]),
  route("admin", "routes/admin-layout.tsx", [
    index("routes/admin-index.tsx"),
    route("general/roles", "routes/admin-general-roles.tsx"),
    route("settings/llms", "routes/admin-settings-llms.tsx"),
    route("se/people", "routes/admin-se-people.tsx"),
    route(
      "se/people/person/:companyId/:personId",
      "routes/admin-se-people-person.tsx",
    ),
    // One company, five tabs. The layout owns the header and the sub-menu; a
    // bare /admin/se/company/:companyId redirects to Info.
    route("se/company/:companyId", "routes/admin-se-company-layout.tsx", [
      index("routes/admin-se-company-index.tsx"),
      route("info", "routes/admin-se-company-info.tsx"),
      route("address", "routes/admin-se-company-address.tsx"),
      route("financial", "routes/admin-se-company-financial.tsx"),
      route("people", "routes/admin-se-company-people.tsx"),
      route("domains", "routes/admin-se-company-domains.tsx"),
    ]),
    route("se/company-info", "routes/admin-se-company-info-table.tsx"),
    route(
      "se/company-info/corrections",
      "routes/admin-se-company-info-corrections.tsx",
    ),
    route(
      "se/company-info/pipeline",
      "routes/admin-se-company-info-pipeline.tsx",
    ),
    route(
      "se/people/llm-input/:draftTwoId",
      "routes/admin-se-people-llm-input.tsx",
    ),
    route(
      "se/people/draft-1-job",
      "routes/admin-se-people-draft-job.ts",
    ),
    route(
      "se/people/draft-2-job",
      "routes/admin-se-people-draft-two-job.ts",
    ),
    route(
      "se/people/llm-bulk-job",
      "routes/admin-se-people-llm-bulk-job.ts",
    ),
    route(
      "se/people/stale-corrections",
      "routes/admin-se-people-stale-corrections.tsx",
    ),
    route("se/people/sources", "routes/admin-se-people-sources.tsx"),
    route(
      "se/people/sources/:sourceName",
      "routes/admin-se-people-source.tsx",
    ),
  ]),
] satisfies RouteConfig;
