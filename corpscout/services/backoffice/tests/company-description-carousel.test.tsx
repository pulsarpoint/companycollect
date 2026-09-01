import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { CompanyDescriptionCarousel } from "~/components/admin/company-description-carousel";
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

describe("CompanyDescriptionCarousel", () => {
  it("renders the first slide with english above the original", () => {
    const html = renderToStaticMarkup(
      <CompanyDescriptionCarousel proposals={descriptionProposals(ARTIFACTS)} />,
    );

    expect(html).toContain("About the company");
    expect(html).toContain("1 / 4");
    const english = html.indexOf(
      "Banking business with deposits and lending.",
    );
    const original = html.indexOf("Bankverksamhet med inlåning och utlåning.");
    expect(english).toBeGreaterThan(-1);
    expect(original).toBeGreaterThan(-1);
    expect(english).toBeLessThan(original);
    // Only the first slide is visible in static markup.
    expect(html).not.toContain("Swedish bank");
  });

  it("renders nothing without proposals", () => {
    const html = renderToStaticMarkup(
      <CompanyDescriptionCarousel proposals={[]} />,
    );
    expect(html).toBe("");
  });
});
