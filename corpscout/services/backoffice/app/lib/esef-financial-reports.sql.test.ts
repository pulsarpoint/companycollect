import { beforeEach, describe, expect, it, vi } from "vitest";

const chQuery = vi.fn();
vi.mock("~/lib/clickhouse.server", () => ({
  chQuery: (...args: unknown[]) => chQuery(...args),
}));
const { getEsefFinancialReport } = await import(
  "~/lib/esef-financial-reports.server"
);

const SUMMARY_ROW = {
  lei: "NHBDILHZTYCNBV5UYZ31",
  fxo_id: "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
  entity_name: "Svenska Handelsbanken AB",
  fiscal_year: 2023,
  period_end: "2023-12-31",
  currency: "SEK",
  mapped_fact_count: 9,
  source_fact_count: 292,
  filing_version: 0,
  viewer_url: "",
  source_url: "",
  package_url: "",
  error_count: 0,
  warning_count: 0,
  date_added: "2024-04-24",
};

// Align field names with EsefFactRow (esef-financial-reports.server.ts:30-52)
const FACT_ROW = {
  fact_id: "fact-489",
  concept_qname: "ifrs-full:DisclosureOfFinanceCostExplanatory",
  concept_local_name: "DisclosureOfFinanceCostExplanatory",
  value_kind: "text",
  raw_value: "<p>Note 5</p>",
  amount_original: null,
  amount_usd: null,
  fx_rate_date: "",
  fx_source: "",
  decimals: null,
  period_start: "2023-01-01",
  period_instant: "",
  period_duration_end: "2023-12-31",
  unit: "",
  currency: "",
  dimensions: "",
  language: "sv",
  concept_labels_json: "[]",
  concept_documentation_json: "[]",
  disclosure_blocks_json: JSON.stringify([
    { type: "heading", text: "Note 5" },
    { type: "paragraph", text: "Finance costs consist of interest." },
  ]),
  disclosure_plain_text: "Note 5 Finance costs consist of interest.",
  disclosure_source_record_uid: "abc123",
  disclosure_text_sha256: "89bfb744be7ea2cf",
  disclosure_parser_name: "lxml_html_disclosure",
  disclosure_parser_version: "1",
};

beforeEach(() => {
  chQuery.mockReset();
});

describe("getEsefFinancialReport disclosure join", () => {
  it("probes for esef_disclosures and joins it on document + fact id", async () => {
    chQuery
      .mockResolvedValueOnce([SUMMARY_ROW])
      .mockResolvedValueOnce([
        { name: "esef_disclosures" },
        { name: "esef_document_concept_labels" },
      ])
      .mockResolvedValueOnce([FACT_ROW]);

    const report = await getEsefFinancialReport(
      "se",
      "5020077862",
      "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
    );

    const probeSql = String(chQuery.mock.calls[1][0]);
    expect(probeSql).toContain("'esef_disclosures'");
    expect(probeSql).not.toContain("'esef_fact_disclosures'");

    const factsSql = String(chQuery.mock.calls[2][0]);
    expect(factsSql).toContain("FROM corpscout.esef_disclosures");
    expect(factsSql).not.toContain("esef_fact_disclosures");
    expect(factsSql).toContain("disclosure_kind = 'tagged_fact'");
    expect(factsSql).toContain(
      "disclosures.source_document_id = facts.fxo_id",
    );
    expect(factsSql).toContain("disclosures.source_fact_id = facts.fact_id");
    expect(factsSql).not.toContain("raw_value_sha256");
    expect(factsSql).toContain("text_sha256");

    const fact = report!.facts[0];
    expect(fact.structuredDisclosure).toEqual({
      blocks: [
        { type: "heading", text: "Note 5" },
        { type: "paragraph", text: "Finance costs consist of interest." },
      ],
      plainText: "Note 5 Finance costs consist of interest.",
    });
    expect(fact.disclosureEvidence).toEqual({
      sourceRecordUid: "abc123",
      textSha256: "89bfb744be7ea2cf",
      parserName: "lxml_html_disclosure",
      parserVersion: "1",
    });
  });

  it("degrades to empty disclosure columns when the table is absent", async () => {
    chQuery
      .mockResolvedValueOnce([SUMMARY_ROW])
      .mockResolvedValueOnce([{ name: "esef_document_concept_labels" }])
      .mockResolvedValueOnce([
        {
          ...FACT_ROW,
          disclosure_blocks_json: "",
          disclosure_plain_text: "",
          disclosure_source_record_uid: "",
          disclosure_text_sha256: "",
          disclosure_parser_name: "",
          disclosure_parser_version: "",
        },
      ]);

    const report = await getEsefFinancialReport(
      "se",
      "5020077862",
      "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
    );

    const factsSql = String(chQuery.mock.calls[2][0]);
    expect(factsSql).not.toContain("esef_disclosures");
    expect(report!.facts[0].structuredDisclosure).toBeNull();
    expect(report!.facts[0].disclosureEvidence).toBeNull();
  });

  it("scopes every concept-labels leg to the requested document", async () => {
    chQuery
      .mockResolvedValueOnce([SUMMARY_ROW])
      .mockResolvedValueOnce([
        { name: "esef_disclosures" },
        { name: "esef_document_concept_labels" },
      ])
      .mockResolvedValueOnce([FACT_ROW]);

    await getEsefFinancialReport(
      "se",
      "5020077862",
      "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
    );

    const factsSql = String(chQuery.mock.calls[2][0]);
    // Taxonomy leg, translation leg, and the official-english anti-join must
    // all carry the document filter; three FROM/JOIN reads of the labels
    // table => three document-scoped predicates.
    const scoped = factsSql.match(
      /source_document_id = \{documentId:String\}/g,
    );
    // 1 disclosure subquery (from the join-fix plan) + 3 label legs + 1 outer WHERE... the
    // outer WHERE uses facts.fxo_id, not source_document_id, so expect exactly 4.
    expect(scoped?.length).toBe(4);
  });
});
