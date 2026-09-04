import { renderToStaticMarkup } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { describe, expect, it } from "vitest";
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
    expect(html).toMatch(/data-source="scb"[^]*?Active/);
    expect(html).toMatch(/data-source="ratsit"[^]*?no opinion/);
    // Bolagsverket has a different status, so it offers Use this; SCB is active and does not.
    expect(html).toMatch(/data-source="bolagsverket"[^]*?Use this/);
    expect(html).not.toMatch(/data-source="scb"[^]*?<button[^>]*>Use this/);
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
});
