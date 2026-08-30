import { Buffer } from "node:buffer";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  chQuery: vi.fn(),
  fetchObject: vi.fn(),
  loadSeCompanyShell: vi.fn(),
}));

vi.mock("~/lib/clickhouse.server", () => ({ chQuery: mocks.chQuery }));
vi.mock("~/lib/object-store.server", () => ({
  fetchObject: mocks.fetchObject,
}));
vi.mock("~/lib/se-company-shell.server", () => ({
  loadSeCompanyShell: mocks.loadSeCompanyShell,
}));

const {
  RATSIT_REQUEST_DETAIL_SQL,
  RATSIT_REQUESTS_SQL,
  htmlToRatsitMarkdown,
  listSeRatsitRequests,
  loadSeRatsitRequestDetail,
  parseRatsitResponseEnvelope,
} = await import("~/lib/se-ratsit-results.server");

const REQUEST = {
  company_id: "193407093016",
  batch_id: "5e53617b-9263-5529-8313-70a41661beac",
  outcome: "success",
  selected_at: "2026-08-27 16:45:45.336",
  attempted_at: "2026-08-27 19:44:41.674",
  completed_at: "2026-08-27 19:44:42.511",
  http_status: 200,
  source_url: "https://www.ratsit.se/3407093016",
  source_bucket: "source-sweden-ratsit",
  source_object_key: "raw/batch/response.json",
  content_size_bytes: 14,
  duration_ms: 837,
  attempt_count: 2,
  error_type: "",
  error_message: "",
  temporal_workflow_id: "ratsit/company/193407093016",
  temporal_run_id: "0022b760-f5f5-42ad-a751-360c8720a147",
  recorded_at: "2026-08-27 19:44:42.585",
};

beforeEach(() => {
  mocks.chQuery.mockReset();
  mocks.fetchObject.mockReset();
  mocks.loadSeCompanyShell.mockReset();
});

describe("Ratsit request queries", () => {
  it("deduplicates ReplacingMergeTree rows and binds paging and request identity", () => {
    expect(RATSIT_REQUESTS_SQL).toContain(
      "FROM corpscout.se_company_ratsit_crawl_results AS r FINAL",
    );
    expect(RATSIT_REQUESTS_SQL).toContain(
      "LIMIT {limit:UInt32} OFFSET {offset:UInt32}",
    );
    expect(RATSIT_REQUEST_DETAIL_SQL).toContain(
      "r.batch_id = {batchId:UUID}",
    );
    expect(RATSIT_REQUEST_DETAIL_SQL).toContain(
      "r.company_id = {companyId:String}",
    );
  });

  it("pages requests and joins their legal names from the Swedish register", async () => {
    mocks.chQuery
      .mockResolvedValueOnce([REQUEST])
      .mockResolvedValueOnce([{ total: "3893" }])
      .mockResolvedValueOnce([
        { company_id: REQUEST.company_id, legal_name: "LINDBLAD, NILS ARNE" },
      ]);

    const page = await listSeRatsitRequests({ page: 2, pageSize: 100 });

    expect(mocks.chQuery).toHaveBeenNthCalledWith(
      1,
      RATSIT_REQUESTS_SQL,
      { limit: 100, offset: 100 },
    );
    expect(mocks.chQuery.mock.calls[2]?.[1]).toEqual({
      companyIds: [REQUEST.company_id],
    });
    expect(page).toMatchObject({ total: 3893, page: 2, pageSize: 100 });
    expect(page.rows[0]).toMatchObject({
      legal_name: "LINDBLAD, NILS ARNE",
      content_size_bytes: 14,
      attempt_count: 2,
    });
  });
});

describe("Ratsit response envelopes", () => {
  it("validates company, batch, and the UTF-8 byte count", () => {
    const content = "<h1>Åsa</h1>";
    const envelope = parseRatsitResponseEnvelope(
      {
        schema_version: 1,
        source: "ratsit",
        browser_id: "proxy1",
        final_url: "https://www.ratsit.se/company-slug",
        content_type: "text/html",
        content,
        result: {
          company_id: REQUEST.company_id,
          batch_id: REQUEST.batch_id,
        },
      },
      {
        company_id: REQUEST.company_id,
        batch_id: REQUEST.batch_id,
        content_size_bytes: Buffer.byteLength(content, "utf8"),
      },
    );

    expect(envelope).toMatchObject({
      browserId: "proxy1",
      finalUrl: "https://www.ratsit.se/company-slug",
      content,
    });
  });

  it("refuses an S3 object belonging to another batch", () => {
    expect(() =>
      parseRatsitResponseEnvelope(
        {
          schema_version: 1,
          source: "ratsit",
          content: "x",
          result: { company_id: REQUEST.company_id, batch_id: "other" },
        },
        {
          company_id: REQUEST.company_id,
          batch_id: REQUEST.batch_id,
          content_size_bytes: 1,
        },
      ),
    ).toThrow(/does not match/);
  });

  it("converts captured HTML to GFM, resolves links, and removes active content", () => {
    const markdown = htmlToRatsitMarkdown(
      `<h2>Company facts</h2>
       <a href="/company-slug">Ratsit profile</a>
       <table><thead><tr><th>Name</th><th>Value</th></tr></thead>
       <tbody><tr><td>Status</td><td>Active</td></tr></tbody></table>
       <table><tbody><tr><td>Address</td><td>Main street</td></tr></tbody></table>
       <script>danger()</script><img src="tracker.gif">`,
      "https://www.ratsit.se/3407093016",
    );

    expect(markdown).toContain("## Company facts");
    expect(markdown).toContain(
      "[Ratsit profile](https://www.ratsit.se/company-slug)",
    );
    expect(markdown).toContain("| Name | Value |");
    expect(markdown).toContain("| Column 1 | Column 2 |");
    expect(markdown).toContain("| Address | Main street |");
    expect(markdown).not.toContain("<table");
    expect(markdown).not.toContain("danger");
    expect(markdown).not.toContain("tracker.gif");
  });

  it("loads and converts the selected successful response only", async () => {
    const content = "<h1>Company</h1>";
    mocks.chQuery.mockResolvedValueOnce([
      { ...REQUEST, content_size_bytes: Buffer.byteLength(content, "utf8") },
    ]);
    mocks.loadSeCompanyShell.mockResolvedValue({
      company_id: REQUEST.company_id,
      legal_name: "LINDBLAD, NILS ARNE",
      legal_form_code: "",
      legal_form_label_en: "",
      legal_form_label_sv: "",
      status: "active",
      incorporation_date: "",
      published: true,
      entity_type_label: "Sole trader",
      is_public_sector: false,
    });
    mocks.fetchObject.mockResolvedValue(
      new Response(
        JSON.stringify({
          schema_version: 1,
          source: "ratsit",
          browser_id: "proxy3",
          final_url: "https://www.ratsit.se/company-slug",
          content_type: "text/html",
          content,
          result: {
            company_id: REQUEST.company_id,
            batch_id: REQUEST.batch_id,
          },
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      ),
    );

    const detail = await loadSeRatsitRequestDetail({
      companyId: REQUEST.company_id,
      batchId: REQUEST.batch_id,
    });

    expect(mocks.chQuery).toHaveBeenCalledWith(RATSIT_REQUEST_DETAIL_SQL, {
      companyId: REQUEST.company_id,
      batchId: REQUEST.batch_id,
    });
    expect(mocks.fetchObject).toHaveBeenCalledWith(
      REQUEST.source_bucket,
      REQUEST.source_object_key,
    );
    expect(detail?.payload).toMatchObject({
      browserId: "proxy3",
      finalUrl: "https://www.ratsit.se/company-slug",
      markdown: "# Company",
    });
    expect(detail?.request.legal_name).toBe("LINDBLAD, NILS ARNE");
  });
});
