import { AddDomainWebtechQueue } from "~/components/admin/add-domain-webtech-queue";
import { WebtechSection } from "~/components/detail/webtech-section";
import { seDomainHref } from "~/lib/se-domains-filters";
import type { getDomainWebtech } from "~/lib/webtech.server";

export function DomainWebtechView({ data }: {
  data: Awaited<ReturnType<typeof getDomainWebtech>>;
}) {
  return (
    <div className="flex flex-col gap-5">
      <AddDomainWebtechQueue
        key={data.domain}
        domain={data.domain}
        actionPath={`${seDomainHref(data.domain)}/web-technologies`}
      />
      <WebtechSection data={data} linkTechnologies />
    </div>
  );
}
