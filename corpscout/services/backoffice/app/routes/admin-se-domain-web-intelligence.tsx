import type { Route } from "./+types/admin-se-domain-web-intelligence";
import { WebIntelligenceSection } from "~/components/detail/web-intelligence-section";
import { getDomainWebIntelligence } from "~/lib/web-intelligence.server";

export async function loader({ params }: Route.LoaderArgs) {
  return getDomainWebIntelligence(params.domain.trim().toLowerCase());
}

export default function DomainWebIntelligence({
  loaderData,
}: Route.ComponentProps) {
  return <WebIntelligenceSection intelligence={loaderData} />;
}
