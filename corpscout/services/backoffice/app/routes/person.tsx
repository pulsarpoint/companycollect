import { redirect } from "react-router";
import type { Route } from "./+types/person";

/** Name URLs are searches now; a name is never treated as a person identity. */
export function loader({ params }: Route.LoaderArgs) {
  const name = decodeURIComponent(params.name).trim();
  return redirect(`/people?q=${encodeURIComponent(name)}`);
}

export default function LegacyPersonNameRedirect() {
  return null;
}
