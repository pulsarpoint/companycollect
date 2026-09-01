import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { CompanyDescriptionCard } from "~/components/admin/company-description-card";
import { descriptionProposals } from "~/lib/se-company-info-payload";

const ARTIFACTS = [
  {
    source: "scb" as const,
    source_record_uid: "scb-1",
    observed_at: "2026-08-01T10:00:00.000Z",
    payload: {
      legal_name: "Svenska Handelsbanken AB",
      activity_description: "Bankverksamhet med inlåning och utlåning.",
      activity_description_en: "Banking business with deposits and lending.",
    },
  },
  {
    source: "esef" as const,
    source_record_uid: "esef-1",
    observed_at: "2026-08-26T10:00:00.000Z",
    payload: {
      company_description: "Handelsbanken is a Swedish credit institution.",
      description_language: "en",
      fiscal_year: "2023",
    },
  },
  {
    source: "esef" as const,
    source_record_uid: "esef-2",
    observed_at: "2026-08-27T10:00:00.000Z",
    payload: {
      company_description: "Handelsbanken är en svensk bank.",
      description_language: "sv",
      fiscal_year: "2024",
    },
  },
  {
    source: "wikidata" as const,
    source_record_uid: "wd-1",
    observed_at: "2026-08-10T10:00:00.000Z",
    payload: {
      company_description: "Swedish bank",
    },
  },
];

describe("descriptionProposals", () => {
  it("splits every source into english-first and original blocks", () => {
    const proposals = descriptionProposals(ARTIFACTS);

    expect(proposals).toHaveLength(4);
    expect(proposals[0]).toMatchObject({
      source: "scb",
      english: "Banking business with deposits and lending.",
      original: "Bankverksamhet med inlåning och utlåning.",
      originalLanguage: "sv",
    });
    expect(proposals[1]).toMatchObject({
      source: "esef",
      english: "Handelsbanken is a Swedish credit institution.",
      original: "",
      meta: "fiscal 2023",
    });
    expect(proposals[2]).toMatchObject({
      source: "esef",
      english: "",
      original: "Handelsbanken är en svensk bank.",
      originalLanguage: "sv",
      meta: "fiscal 2024",
    });
    expect(proposals[3]).toMatchObject({
      source: "wikidata",
      original: "Swedish bank",
      originalLanguage: "",
    });
  });

  it("skips rows without any description and dedupes identical proposals", () => {
    const proposals = descriptionProposals([
      {
        source: "scb" as const,
        source_record_uid: "empty",
        observed_at: "2026-08-01T10:00:00.000Z",
        payload: { legal_name: "No description here" },
      },
      ARTIFACTS[1],
      { ...ARTIFACTS[1], source_record_uid: "esef-duplicate" },
    ]);

    expect(proposals).toHaveLength(1);
  });
});

describe("CompanyDescriptionCard", () => {
  it("offers one menu option per proposal, disambiguated when a source repeats", () => {
    const html = renderToStaticMarkup(
      <CompanyDescriptionCard proposals={descriptionProposals(ARTIFACTS)} />,
    );

    expect(html).toContain("About the company");
    // Menu options for every source of this company; the repeated ESEF
    // source carries its meta so both years are selectable.
    expect(html).toContain("SCB register");
    expect(html).toContain("ESEF filing · fiscal 2023");
    expect(html).toContain("ESEF filing · fiscal 2024");
    expect(html).toContain("Wikidata");
    // English sits above the original inside a proposal's content.
    const english = html.indexOf(
      "Banking business with deposits and lending.",
    );
    const original = html.indexOf("Bankverksamhet med inlåning och utlåning.");
    expect(english).toBeGreaterThan(-1);
    expect(original).toBeGreaterThan(-1);
    expect(english).toBeLessThan(original);
  });

  it("renders nothing without proposals", () => {
    const html = renderToStaticMarkup(
      <CompanyDescriptionCard proposals={[]} />,
    );
    expect(html).toBe("");
  });
});
