import { beforeEach, describe, expect, it, vi } from "vitest";

const clickhouse = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("~/lib/clickhouse.server", () => ({ chQuery: clickhouse.query }));

import {
  ESEF_REPORT_NOTES_SQL,
  getEsefReportNotes,
} from "~/lib/esef-report-notes.server";

beforeEach(() => clickhouse.query.mockReset());

describe("ESEF_REPORT_NOTES_SQL", () => {
  it("reads tagged_fact disclosures for one document in visual order", () => {
    expect(ESEF_REPORT_NOTES_SQL).toContain("FROM corpscout.esef_disclosures");
    expect(ESEF_REPORT_NOTES_SQL).toContain(
      "disclosure_kind = 'tagged_fact'",
    );
    expect(ESEF_REPORT_NOTES_SQL).toContain(
      "source_document_id = {documentId:String}",
    );
    expect(ESEF_REPORT_NOTES_SQL).toContain("ORDER BY anchor_visual_order");
  });
});

describe("getEsefReportNotes", () => {
  it("parses persisted blocks and falls back to plain text", async () => {
    clickhouse.query
      .mockResolvedValueOnce([
        {
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
        },
      ])
      .mockResolvedValueOnce([
        {
          disclosure_id: "c64d2fa8",
          concept_qname: "ifrs-full:DisclosureOfFinanceCostExplanatory",
          concept_local_name: "DisclosureOfFinanceCostExplanatory",
          language: "sv",
          printed_page_number: "112",
          block_count: 5,
          table_count: 2,
          original_character_count: 900,
          blocks_json: JSON.stringify([{ type: "heading", text: "Not 5" }]),
          plain_text: "Not 5",
        },
        {
          disclosure_id: "broken",
          concept_qname: "ifrs-full:DisclosureOfDebtSecuritiesExplanatory",
          concept_local_name: "DisclosureOfDebtSecuritiesExplanatory",
          language: "sv",
          printed_page_number: "",
          block_count: 0,
          table_count: 0,
          original_character_count: 10,
          blocks_json: "not json",
          plain_text: "fallback text",
        },
      ]);

    const notes = await getEsefReportNotes(
      "se",
      "5020077862",
      "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
    );
    expect(notes?.notes[0].disclosure.blocks).toEqual([
      { type: "heading", text: "Not 5" },
    ]);
    // invalid persisted JSON degrades to a single paragraph of plain text
    expect(notes?.notes[1].disclosure).toEqual({
      blocks: [{ type: "paragraph", text: "fallback text" }],
      plainText: "fallback text",
    });
  });
});
