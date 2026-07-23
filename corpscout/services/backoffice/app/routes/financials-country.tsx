import { redirect } from "react-router";
import type { Route } from "./+types/financials-country";
import { getCountry } from "~/lib/countries";

export async function loader({ params }: Route.LoaderArgs) {
  if (!getCountry(params.country)) throw new Response("Country not found", { status: 404 });
  throw redirect(`/countries/${params.country}`);
}

export default function FinancialsCountryRedirect() {
  return null;
}
