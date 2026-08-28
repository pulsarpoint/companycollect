import { CpuIcon } from "lucide-react";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";

/**
 * The admin technology area never 404s the way its public twins do: a
 * reviewer opening any technology sub-tab for a company with no data needs
 * the page to say "nothing yet", not vanish. Every sub-tab renders this in
 * place of its section when its loader has nothing to show.
 */
export function SeCompanyTechnologyEmpty({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <Empty className="border">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <CpuIcon />
        </EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{description}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

/** The one empty state every sub-tab shares: no source ever resolved a
 * domain, so nothing domain-scoped can exist. */
export function SeCompanyTechnologyNoDomains() {
  return (
    <SeCompanyTechnologyEmpty
      title="No domains resolved for this company"
      description="Technology, infrastructure, DNS, and IP evidence is scoped
        to a company's domains, and no source has suggested a domain for this
        company."
    />
  );
}
