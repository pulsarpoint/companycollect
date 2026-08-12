import { useEffect, useRef, useState } from "react";
import { Link, useFetcher } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import { Skeleton } from "~/components/ui/skeleton";
import { ContactLocationCard } from "~/components/detail/contact-location-card";
import { GleifGroupSection } from "~/components/detail/gleif-group-section";
import { IndustriesSection } from "~/components/detail/industries-section";
import { ManagementSection } from "~/components/detail/management-section";
import { PublicContractsSection } from "~/components/detail/public-contracts-section";
import {
  ContactsDomainsSection,
  DescriptionsSection,
  SourcesSection,
} from "~/components/detail/source-information-sections";
import { WikidataSection } from "~/components/detail/wikidata-section";
import type { CountryConfig } from "~/lib/countries";
import type { CompanySectionName } from "~/lib/company-sections.server";
import type { CompanySectionResource } from "~/routes/company-section";

function SectionSkeleton() {
  return (
    <Card aria-busy="true">
      <CardHeader>
        <Skeleton className="h-5 w-40" />
      </CardHeader>
      <CardContent className="space-y-3">
        <Skeleton className="h-4 w-full" />
        <Skeleton className="h-4 w-4/5" />
      </CardContent>
    </Card>
  );
}

export function LazyCompanySection({
  section,
  country,
  companyId,
  record,
}: {
  section: CompanySectionName;
  country: CountryConfig;
  companyId: string;
  record: Record<string, unknown>;
}) {
  const fetcher = useFetcher<CompanySectionResource>();
  const container = useRef<HTMLDivElement>(null);
  const requested = useRef(false);
  const [nearViewport, setNearViewport] = useState(false);

  useEffect(() => {
    const element = container.current;
    if (!element) return;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setNearViewport(true);
          observer.disconnect();
        }
      },
      { rootMargin: "320px 0px" },
    );
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (!nearViewport || requested.current) return;
    requested.current = true;
    fetcher.load(
      `/company/${country.code}/${encodeURIComponent(companyId)}/section/${section}`,
    );
  }, [companyId, country.code, fetcher, nearViewport, section]);

  const response = fetcher.data;
  return (
    <div ref={container} className="min-h-28">
      {!response ? (
        <SectionSkeleton />
      ) : !response.ok ? (
        <Alert variant="destructive">
          <AlertTitle>Section unavailable</AlertTitle>
          <AlertDescription>{response.error}</AlertDescription>
        </Alert>
      ) : (
        <CompanySection
          data={response.data}
          country={country}
          companyId={companyId}
          record={record}
        />
      )}
    </div>
  );
}

function CompanySection({
  data,
  country,
  companyId,
  record,
}: {
  data: Extract<CompanySectionResource, { ok: true }>["data"];
  country: CountryConfig;
  companyId: string;
  record: Record<string, unknown>;
}) {
  switch (data.section) {
    case "gleif":
      return (
        <GleifGroupSection
          entity={data.entity}
          relationships={data.relationships}
          countryCode={country.code}
        />
      );
    case "wikidata":
      return <WikidataSection wikidata={data.wikidata} />;
    case "management":
      return (
        <ManagementSection
          officers={data.officers}
          peopleMatches={[]}
          audit={null}
          wikidataPeople={data.wikidataPeople}
          esefPeople={data.esefPeople}
        />
      );
    case "descriptions":
      return <DescriptionsSection descriptions={data.descriptions} />;
    case "domains":
      return (
        <ContactsDomainsSection
          contacts={[]}
          domains={data.domains}
          sourceContacts={data.sourceContacts}
          wikidata={null}
        />
      );
    case "contracts":
      return <PublicContractsSection contracts={data.contracts} summary={data.summary} />;
    case "financials":
      return data.available ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Financial statements</CardTitle>
          </CardHeader>
          <CardContent className="flex items-center justify-between gap-4">
            <p className="text-muted-foreground text-sm">
              Filed company-keyed financial snapshots and yearly history are available.
            </p>
            <Button
              variant="outline"
              nativeButton={false}
              render={<Link to={`/company/${country.code}/${companyId}/financials`} />}
            >
              View financials
            </Button>
          </CardContent>
        </Card>
      ) : null;
    case "industries":
      return <IndustriesSection industries={data.industries} />;
    case "addresses":
      return (
        <ContactLocationCard
          country={country}
          companyId={companyId}
          contacts={[]}
          addresses={data.addresses}
          record={record}
        />
      );
    case "sources":
      return <SourcesSection records={data.records} />;
    case "technology":
      return null;
  }
}
