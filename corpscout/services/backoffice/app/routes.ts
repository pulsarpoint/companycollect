import { type RouteConfig, index, layout, route } from "@react-router/dev/routes";

export default [
  layout("routes/shell.tsx", [
    index("routes/home.tsx"),
    route("companies", "routes/companies.tsx"),
    route("countries", "routes/countries.tsx"),
    route("countries/:country/companies", "routes/country-companies.tsx"),
    route("countries/:country/contracts/:ref", "routes/country-contract-detail.tsx"),
    route("countries/:country", "routes/country-overview.tsx"),
    route("company/:country/:id", "routes/country-company-detail.tsx"),
    route("company/:country/:id/facts/:year", "routes/company-facts.tsx"),
    route("company/:country/:id/facts/:year/document", "routes/company-facts-document.ts"),
    route("company/:country/geocode", "routes/country-geocode.ts"),
    route("facet-options", "routes/facet-options.ts"),
    // Deliberately not nested under a country: a source page shows the whole
    // register, including the winners no country view will ever carry.
    route("procurements", "routes/procurements.tsx"),
    route("procurements/:source", "routes/procurement-source.tsx"),
    route("procurements/:source/:key", "routes/procurement-record.tsx"),
    route("people", "routes/people.tsx"),
    route("person/:name", "routes/person.tsx"),
    route("financials", "routes/financials.tsx"),
    route("financials/country/:country", "routes/financials-country.tsx"),
    route("financials/industry/:division", "routes/financials-industry.tsx"),
  ]),
] satisfies RouteConfig;
