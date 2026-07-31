import { type RouteConfig, index, layout, route } from "@react-router/dev/routes";

export default [
  layout("routes/shell.tsx", [
    index("routes/home.tsx"),
    route("countries", "routes/countries.tsx"),
    route("countries/:country/companies", "routes/country-companies.tsx"),
    route("countries/:country/contracts/:ref", "routes/country-contract-detail.tsx"),
    route("countries/:country/facet-options", "routes/country-facet-options.ts"),
    // One level of the CPV tree at a time; see the route for why it is not
    // loader data.
    route("countries/:country/contracts-cpv", "routes/country-contracts-cpv.ts"),
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
      route("financials", "routes/company-financials.tsx"),
      route("financials/:documentId", "routes/company-financial-report.tsx"),
    ]),
    route("company/:country/:id/facts/:year", "routes/company-facts.tsx"),
    route("company/:country/:id/facts/:year/document", "routes/company-facts-document.ts"),
    route("company/:country/geocode", "routes/country-geocode.ts"),
    // Deliberately not nested under a country: a source page shows the whole
    // register, including the winners no country view will ever carry.
    route("procurements", "routes/procurements.tsx"),
    route("procurements/:source", "routes/procurement-source.tsx"),
    route("procurements/:source/:key", "routes/procurement-record.tsx"),
    route("people", "routes/people.tsx"),
    route("person/:name", "routes/person.tsx"),
    // Old bookmarks: /financials/country/:c still lands on the country page.
    route("financials/country/:country", "routes/financials-country.tsx"),
  ]),
] satisfies RouteConfig;
