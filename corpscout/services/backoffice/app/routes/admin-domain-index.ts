import { redirect } from "react-router";
import type { Route } from "./+types/admin-domain-index";

export function loader({ params, request }: Route.LoaderArgs) {
  return redirect(`/admin/domains/${encodeURIComponent(params.domain.trim().toLowerCase())}/dns${new URL(request.url).search}`);
}
