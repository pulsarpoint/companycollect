import type { Route } from "./+types/admin-se-domain-mail-security";
import {
  MailSecurityNoRecords,
  MailSecuritySection,
} from "~/components/detail/mail-security-section";
import { getDomainMailSecurity } from "~/lib/se-company-mail-security.server";

export async function loader({ params }: Route.LoaderArgs) {
  return getDomainMailSecurity(params.domain.trim().toLowerCase());
}

export default function DomainMailSecurity({
  loaderData,
}: Route.ComponentProps) {
  return loaderData.recordCount === 0 ? (
    <MailSecurityNoRecords domain={loaderData.domain} />
  ) : (
    <MailSecuritySection report={loaderData.report} />
  );
}
