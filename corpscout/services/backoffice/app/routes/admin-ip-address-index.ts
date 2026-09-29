import { redirect } from "react-router";
import type { Route } from "./+types/admin-ip-address-index";

export function loader({ params, request }: Route.LoaderArgs) {
  return redirect(`/admin/ip-addresses/${encodeURIComponent(params.address)}/overview${new URL(request.url).search}`);
}
