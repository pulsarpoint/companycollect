import { type RouteConfig, index, layout, route } from "@react-router/dev/routes";

export default [
  layout("routes/shell.tsx", [
    index("routes/home.tsx"),
    route("companies", "routes/companies.tsx"),
    route("company/:country/:id", "routes/country-company-detail.tsx"),
    route("company/:country/geocode", "routes/country-geocode.ts"),
    route("facet-options", "routes/facet-options.ts"),
    route("financials", "routes/financials.tsx"),
    route("financials/country/:country", "routes/financials-country.tsx"),
    route("financials/industry/:division", "routes/financials-industry.tsx"),
  ]),
] satisfies RouteConfig;
