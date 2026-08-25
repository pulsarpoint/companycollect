import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  ContactsDomainsSection,
  DescriptionsSection,
  ProductsMarketsSection,
  SourcesSection,
} from "~/components/detail/source-information-sections";
import type {
  CompanyDescriptionObservation,
  EvidenceRef,
} from "~/lib/queries.server";
import {
  COMPANY_DESCRIPTION_OBSERVATIONS_QUERY,
  COMPANY_SOURCE_RECORDS_QUERY,
  ESEF_DOCUMENT_BUSINESS_ITEMS_QUERY,
  ESEF_DOCUMENT_CONTACTS_QUERY,
  ESEF_DOCUMENT_PEOPLE_QUERY,
  ESEF_DOCUMENT_RELATIONSHIPS_QUERY,
} from "~/lib/queries.server";

function evidence(uid: string, sourceSlug: string): EvidenceRef {
  return {
    sourceRecordUid: uid,
    recordKind: "registry_company",
    contentSha256: "a".repeat(64),
    firstSeenAt: "2025-01-01 00:00:00",
    lastSeenAt: "2026-01-01 00:00:00",
    origins: [
      {
        sourceSlug,
        sourceRecordKey: "5566692850",
        sourceUrl: `https://example.test/${sourceSlug}`,
        sourceObjectKey: "source/object.json",
        payloadSha256: "a".repeat(64),
        retrievedAt: "2026-01-01 00:00:00",
        sourceRunId: "run-1",
      },
    ],
  };
}

function description(
  uid: string,
  sourceSlug: string,
  extractedAt: string,
): CompanyDescriptionObservation {
  return {
    observationUid: `observation-${uid}-${extractedAt}`,
    sourceRecordUid: uid,
    descriptionKind: "registered_activity",
    textOriginal: "Same description remains independently visible.",
    languageOriginal: "en",
    textEn: "Same description remains independently visible.",
    extractedAt,
    evidence: [evidence(uid, sourceSlug)],
  };
}

describe("source-preserving company information", () => {
  it("keeps identical descriptions from independent sources separate", () => {
    const html = renderToStaticMarkup(
      <DescriptionsSection
        descriptions={[
          description("a".repeat(64), "sweden_bolagsverket", "2026-01-01"),
          description("b".repeat(64), "wikidata", "2026-01-01"),
        ]}
      />,
    );

    expect(html.match(/Same description remains independently visible\./g)).toHaveLength(2);
    expect(html).toContain("sweden bolagsverket");
    expect(html).toContain("wikidata");
  });

  it("renders older versions behind an expandable history", () => {
    const html = renderToStaticMarkup(
      <DescriptionsSection
        descriptions={[
          description("a".repeat(64), "wikidata", "2026-01-01"),
          description("b".repeat(64), "wikidata", "2025-01-01"),
        ]}
      />,
    );

    expect(html).toContain("Older versions (1)");
    expect(html).toContain("Evidence (1)");
  });

  it("renders contacts, business details, and auditable source records", () => {
    const source = evidence("c".repeat(64), "esef_filings");
    const html = [
      renderToStaticMarkup(
        <ContactsDomainsSection
          contacts={[]}
          domains={[]}
          wikidata={null}
          sourceContacts={[
            {
              candidateId: "contact-1",
              sourceRecordUid: source.sourceRecordUid,
              fiscalYear: 2025,
              candidateKind: "domain",
              normalizedValue: "aak.com",
              registrableDomain: "aak.com",
              evidence: [source],
            },
          ]}
        />,
      ),
      renderToStaticMarkup(
        <ProductsMarketsSection
          items={[
            {
              candidateUid: "item-1",
              sourceRecordUid: source.sourceRecordUid,
              fiscalYear: 2025,
              itemKind: "product_or_service",
              name: "Plant-based oils",
              geographyType: "",
              evidence: [source],
            },
          ]}
        />,
      ),
      renderToStaticMarkup(
        <SourcesSection
          records={[
            {
              sourceRecordUid: source.sourceRecordUid,
              recordKind: source.recordKind,
              firstSeenAt: source.firstSeenAt,
              lastSeenAt: source.lastSeenAt,
              evidence: [source],
            },
          ]}
        />,
      ),
    ].join("\n");

    expect(html).toContain("Contacts and domains");
    expect(html).toContain("Products and markets");
    expect(html).toContain("Sources");
    expect(html).toContain("aak.com");
    expect(html).toContain("Plant-based oils");
    expect(html).toContain("Source URL");
  });

  it("queries source-record evidence through company-keyed lookups", () => {
    expect(COMPANY_SOURCE_RECORDS_QUERY).toContain(
      "company_section_item_source_links",
    );
    expect(COMPANY_SOURCE_RECORDS_QUERY).toContain(
      "country_code = {country:String} AND company_id = {id:String}",
    );
    expect(COMPANY_SOURCE_RECORDS_QUERY).not.toContain(" JOIN ");
    expect(COMPANY_DESCRIPTION_OBSERVATIONS_QUERY).toContain(
      "company_description_current",
    );
    expect(ESEF_DOCUMENT_PEOPLE_QUERY).toContain("esef_document_people FINAL");
    expect(ESEF_DOCUMENT_BUSINESS_ITEMS_QUERY).toContain(
      "esef_document_business_items FINAL",
    );
    expect(ESEF_DOCUMENT_RELATIONSHIPS_QUERY).toContain(
      "esef_document_group_relationships FINAL",
    );
    expect(ESEF_DOCUMENT_CONTACTS_QUERY).toContain("esef_document_contact_candidates");
  });
});
