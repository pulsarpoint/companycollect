import { type RouteConfig, index, route } from "@react-router/dev/routes";

export default [
  index("routes/home.tsx"),
  route(":country", "routes/country.tsx", [
    index("routes/country-overview.tsx"),
    route("companies", "routes/country-companies.tsx"),
  ]),
] satisfies RouteConfig;
