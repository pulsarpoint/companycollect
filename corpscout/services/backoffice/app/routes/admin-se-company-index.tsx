import { redirect } from "react-router";
import type { Route } from "./+types/admin-se-company-index";

/**
 * The company area has no landing page of its own: Info is the tab a reviewer
 * wants, and a bare `/admin/se/company/:companyId` (an old bookmark, or a
 * link that dropped the tab) lands there rather than on an empty frame.
 */
export function loader({ params }: Route.LoaderArgs) {
  throw redirect(
    `/admin/se/company/${encodeURIComponent(params.companyId)}/info`,
  );
}

export default function AdminSwedenCompanyIndex() {
  // Unreachable: the loader always redirects. Present because a route module
  // without a default export cannot be rendered if that ever changes.
  return null;
}
