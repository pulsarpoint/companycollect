import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const server = vi.hoisted(() => ({
  loadSeBasicInfoDetail: vi.fn(),
  appendSeBasicInfoReviewerDecision: vi.fn(),
  launchSeBasicInfoFold: vi.fn(),
  SeBasicInfoDecisionError: class SeBasicInfoDecisionError extends Error {},
}));
vi.mock("~/lib/se-basic-info.server", () => server);

import { action, loader } from "~/routes/admin-se-company-info";
import {
  SeBasicInfoNotFolded,
  SeBasicInfoWorkspace,
} from "~/components/admin/se-basic-info-workspace";
import type { SeBasicInfoDetail, SeBasicInfoSuggestionRow } from "~/lib/se-basic-info.server";

const COMPANY = "0113004022";

const bolagsverket: SeBasicInfoSuggestionRow = {
  company_id: COMPANY,
  source: "bolagsverket",
  source_record_uid: "abc",
  observed_at: "2026-09-03 18:16:21.117",
  suggested_at: "2026-09-04 17:46:53.852",
  legal_name: "Sportstugan upa",
  legal_form_code: "51",
  status: "inactive",
  incorporation_date: "1937-05-12",
  lei: "",
  wikidata_id: "",
  description: "Förvaltar fastigheter.",
  description_language: "sv",
  description_sv: "Förvaltar fastigheter.",
  decided_by: "",
  note: "",
  source_run_id: "run-b",
  extractor_version: "bolagsverket-v2",
};
const scb: SeBasicInfoSuggestionRow = {
  ...bolagsverket,
  source: "scb",
  legal_form_code: "51",
  status: "active",
  description: "",
  description_language: "",
  description_sv: "",
  suggested_at: "2026-09-04 11:20:00.000",
};

const detail: SeBasicInfoDetail = {
  info: {
    company_id: COMPANY,
    legal_name: "Sportstugan upa",
    legal_name_source: "scb",
    legal_form_code: "51",
    legal_form_code_source: "scb",
    status: "active",
    status_source: "scb",
    incorporation_date: "1937-05-12",
    incorporation_date_source: "scb",
    lei: "",
    lei_source: "",
    wikidata_id: "",
    wikidata_id_source: "",
    description: "Förvaltar fastigheter.",
    description_source: "bolagsverket",
    description_language: "sv",
    description_sv: "Förvaltar fastigheter.",
    description_sv_source: "bolagsverket",
    folded_at: "2026-09-04 17:04:01.293",
    fold_version: "fold-v1",
    source_run_id: "run-f",
  },
  suggestions: [bolagsverket, scb],
  history: [],
  precedence: [
    { field: "status", source: "reviewer", precedence: 10000 },
    { field: "status", source: "scb", precedence: 1000 },
    { field: "status", source: "bolagsverket", precedence: 900 },
    { field: "status", source: "ratsit", precedence: 300 },
  ],
  legalFormLabels: { "51": { label_en: "Economic association (ekonomisk förening)", label_sv: "Ekonomisk förening" } },
  foldPending: true,
};

function render(element: React.ReactElement, search = ""): string {
  const router = createMemoryRouter([{ path: "/admin/se/company/:companyId/info", element }], {
    initialEntries: [`/admin/se/company/${COMPANY}/info${search}`],
  });
  return renderToStaticMarkup(<RouterProvider router={router} />);
}

describe("SeBasicInfoWorkspace", () => {
  it("lists every field with its winning source and marks the selected row", () => {
    const html = render(<SeBasicInfoWorkspace detail={detail} selectedField="status" result={null} />, "?field=status");
    for (const label of ["Legal name", "Legal form", "Status", "Incorporated", "LEI", "Wikidata", "Description", "Description (Swedish)"]) {
      expect(html).toContain(label);
    }
    expect(html).toContain("Ekonomisk förening");
    expect(html).toContain('aria-current="true"');
    expect(html).toContain("fold-v1");
    expect(html).toContain("2026-09-04 17:04:01.293");
  });

  it("orders the panel by precedence, marks the winner active and greys silent sources", () => {
    const html = render(<SeBasicInfoWorkspace detail={detail} selectedField="status" result={null} />, "?field=status");
    const scbAt = html.indexOf('data-source="scb"');
    const bolagsverketAt = html.indexOf('data-source="bolagsverket"');
    const ratsitAt = html.indexOf('data-source="ratsit"');
    const reviewerAt = html.indexOf('data-source="reviewer"');
    expect(reviewerAt).toBeLessThan(scbAt);
    expect(scbAt).toBeLessThan(bolagsverketAt);
    expect(bolagsverketAt).toBeLessThan(ratsitAt);
    expect(html).toMatch(/data-source="ratsit"[^]*?no opinion/);
    // Bolagsverket has a different status, so it offers Use this; SCB is active and does not.
    // Slice each row out of the document so these checks read only that row's
    // own markup -- toMatch/toContain over the whole string would happily
    // find another row's later button and pass for the wrong reason.
    const scbRow = html.slice(scbAt, bolagsverketAt);
    expect(scbRow).toContain("Active");
    expect(scbRow).not.toContain("Use this");
    const bolagsverketRow = html.slice(bolagsverketAt, ratsitAt);
    expect(bolagsverketRow).toContain("Use this");
  });

  it("shows the fold-pending alert with Fold now, and the poller after a launch", () => {
    const html = render(<SeBasicInfoWorkspace detail={detail} selectedField="legal_name" result={null} />);
    expect(html).toContain("Fold pending");
    expect(html).toContain('value="fold-now"');
    const launched = render(
      <SeBasicInfoWorkspace detail={detail} selectedField="legal_name" result={{ ok: true, launched: { runId: "run-9", url: null } }} />,
    );
    expect(launched).toContain("run-9");
    const settled = render(<SeBasicInfoWorkspace detail={{ ...detail, foldPending: false }} selectedField="legal_name" result={null} />);
    expect(settled).not.toContain("Fold pending");
  });

  it("renders an error result and the not-folded state", () => {
    expect(render(<SeBasicInfoWorkspace detail={detail} selectedField="lei" result={{ ok: false, error: "Unknown source." }} />)).toContain("Unknown source.");
    expect(renderToStaticMarkup(<SeBasicInfoNotFolded companyId={COMPANY} />)).toContain("not in se_company_basic_info yet");
  });

  it("offers Use this when the company has no main row", () => {
    const html = render(
      <SeBasicInfoWorkspace detail={{ ...detail, info: null, foldPending: true }} selectedField="status" result={null} />,
      "?field=status",
    );
    expect(html).toContain("Not folded yet");
    const bolagsverketAt = html.indexOf('data-source="bolagsverket"');
    const ratsitAt = html.indexOf('data-source="ratsit"');
    const bolagsverketRow = html.slice(bolagsverketAt, ratsitAt);
    expect(bolagsverketRow).not.toContain("Active");
    expect(bolagsverketRow).toContain("Use this");
  });

  it("a reviewer row that lost to the pending fold offers Release, not Use this", () => {
    const reviewerRow: SeBasicInfoSuggestionRow = {
      ...bolagsverket,
      source: "reviewer",
      status: "inactive",
      decided_by: "backoffice",
      note: "keep it",
      suggested_at: "2026-09-04 18:00:00.000",
    };
    const withReviewer: SeBasicInfoDetail = {
      ...detail,
      suggestions: [...detail.suggestions, reviewerRow],
    };
    const html = render(<SeBasicInfoWorkspace detail={withReviewer} selectedField="status" result={null} />, "?field=status");
    const reviewerAt = html.indexOf('data-source="reviewer"');
    const scbAt = html.indexOf('data-source="scb"');
    const reviewerRowHtml = html.slice(reviewerAt, scbAt);
    expect(reviewerRowHtml).toContain("Release");
    expect(reviewerRowHtml).toContain("keep it");
    expect(reviewerRowHtml).not.toContain("Use this");
    expect(reviewerRowHtml).not.toContain("Active");
  });
});

describe("admin-se-company-info route", () => {
  beforeEach(() => {
    server.loadSeBasicInfoDetail.mockReset().mockResolvedValue(detail);
    server.appendSeBasicInfoReviewerDecision.mockReset().mockResolvedValue({ suggestedAt: "2026-09-04 19:30:00.123" });
    server.launchSeBasicInfoFold.mockReset().mockResolvedValue({ runId: "run-9", url: null });
  });

  it("loads the detail and the selected field from the URL", async () => {
    const response = await loader({
      request: new Request(`http://x/admin/se/company/${COMPANY}/info?field=status`),
      params: { companyId: COMPANY },
    } as never);
    expect(response.data).toEqual({ detail, selectedField: "status" });
    expect(response.init?.status).toBeUndefined();
    server.loadSeBasicInfoDetail.mockResolvedValueOnce(null);
    const missing = await loader({ request: new Request("http://x/info"), params: { companyId: COMPANY } } as never);
    expect(missing.data).toEqual({ detail: null, selectedField: "legal_name" });
    expect(missing.init?.status).toBe(404);
  });

  it("writes a reviewer decision, launches a fold, and reports refusals", async () => {
    const post = (entries: Record<string, string>) => {
      const body = new FormData();
      for (const [key, value] of Object.entries(entries)) body.set(key, value);
      return action({ request: new Request("http://x/info", { method: "POST", body }), params: { companyId: COMPANY } } as never);
    };
    expect(await post({ intent: "use-this", field: "status", source: "bolagsverket" })).toEqual({ ok: true, suggestedAt: "2026-09-04 19:30:00.123" });
    expect(server.appendSeBasicInfoReviewerDecision).toHaveBeenCalledWith(COMPANY, { intent: "use-this", field: "status", source: "bolagsverket", note: "" });
    expect(await post({ intent: "fold-now" })).toEqual({ ok: true, launched: { runId: "run-9", url: null } });
    expect(await post({ intent: "use-this", field: "status", source: "reviewer" })).toEqual({ ok: false, error: "Use this needs a source other than the reviewer." });
    server.appendSeBasicInfoReviewerDecision.mockRejectedValueOnce(new server.SeBasicInfoDecisionError("SCB has no LEI for this company."));
    expect(await post({ intent: "use-this", field: "lei", source: "scb" })).toEqual({ ok: false, error: "SCB has no LEI for this company." });
    server.appendSeBasicInfoReviewerDecision.mockRejectedValueOnce(new Error("clickhouse down"));
    await expect(post({ intent: "release", field: "lei" })).rejects.toThrow("clickhouse down");
  });
});
