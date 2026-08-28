import type { Route } from "./+types/admin-se-company-technology-mail-security";
import { SeCompanyTechnologyNoDomains } from "~/components/admin/se-company-technology-empty";
import {
  MailSecurityNoRecords,
  MailSecuritySection,
} from "~/components/detail/mail-security-section";
import { getCountry } from "~/lib/countries";
import { getCompanyDomains } from "~/lib/queries.server";
import { getDomainMailSecurity } from "~/lib/se-company-mail-security.server";

// Only `loader` and the component live here -- see
// admin-se-company-layout.tsx for why.

// Admin-only for now: the mail security score is computed live from crawled
// DNS records (no precompute, owner decision) and follows the same
// ?domain= threading as the sibling technology sub-tabs.

export async function loader({ params, request }: Route.LoaderArgs) {
  const domains = await getCompanyDomains(getCountry("se")!, params.companyId);
  const requestedDomain = new URL(request.url).searchParams
    .get("domain")
    ?.trim()
    .toLowerCase()
    .replace(/\.$/, "");
  const selectedDomain =
    domains.find((domain) => domain.domain === requestedDomain) ??
    domains.find((domain) => domain.is_primary === 1) ??
    domains[0];
  if (!selectedDomain) return null;

  return getDomainMailSecurity(selectedDomain.domain);
}

export default function AdminSwedenCompanyTechnologyMailSecurity({
  loaderData,
}: Route.ComponentProps) {
  if (!loaderData) {
    return <SeCompanyTechnologyNoDomains />;
  }
  if (loaderData.recordCount === 0) {
    return <MailSecurityNoRecords domain={loaderData.domain} />;
  }
  return <MailSecuritySection report={loaderData.report} />;
}
