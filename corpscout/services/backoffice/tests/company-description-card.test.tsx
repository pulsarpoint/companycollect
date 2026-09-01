import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  CompanyDescriptionCard,
  displayedBlock,
} from "~/components/admin/company-description-card";
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
      // Carried through so a page can write the proposal back as a decided
      // value with the record and moment it came from.
      sourceRecordUid: "scb-1",
      observedAt: "2026-08-01T10:00:00.000Z",
    });
    expect(proposals[1]).toMatchObject({
      source: "esef",
      english: "Handelsbanken is a Swedish credit institution.",
      original: "",
      meta: "fiscal 2023",
      sourceRecordUid: "esef-1",
      observedAt: "2026-08-26T10:00:00.000Z",
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
  });

  it("shows one language at a time, toggled at the top, with fallback", () => {
    const html = renderToStaticMarkup(
      <CompanyDescriptionCard proposals={descriptionProposals(ARTIFACTS)} />,
    );

    // The language toggle sits once at the top of the card.
    expect(html).toContain('aria-label="Show english descriptions"');
    expect(html).toContain('aria-label="Show original-language descriptions"');
    // Default is english: the active SCB option shows only its english text.
    expect(html).toContain("Banking business with deposits and lending.");
    expect(html).not.toContain("Bankverksamhet med inlåning och utlåning.");
  });

  it("falls back to the language a proposal actually has, chip telling the truth", () => {
    const [scb, esefEn, esefSv, wikidata] = descriptionProposals(ARTIFACTS);

    expect(displayedBlock(scb, "en").chip).toBe("en");
    expect(displayedBlock(scb, "original")).toMatchObject({
      text: "Bankverksamhet med inlåning och utlåning.",
      chip: "sv",
    });
    // English-only proposal keeps its english text under "original".
    expect(displayedBlock(esefEn, "original")).toMatchObject({
      text: "Handelsbanken is a Swedish credit institution.",
      chip: "en",
    });
    // Original-only proposals keep their text under "en".
    expect(displayedBlock(esefSv, "en")).toMatchObject({
      text: "Handelsbanken är en svensk bank.",
      chip: "sv",
    });
    expect(displayedBlock(wikidata, "en")).toMatchObject({
      text: "Swedish bank",
      chip: "original",
    });
  });

  it("renders the action slot under each option's text, naming the field shown", () => {
    const html = renderToStaticMarkup(
      <CompanyDescriptionCard
        proposals={descriptionProposals(ARTIFACTS)}
        renderAction={(proposal, shown) => (
          <span
            data-action={`${proposal.source}:${shown.field}:${shown.text.slice(0, 8)}`}
          />
        )}
      />,
    );

    // Default language is english, so every option that has english text is
    // offering `description`...
    expect(html).toContain('data-action="scb:description:Banking "');
    expect(html).toContain('data-action="esef:description:Handelsb"');
    // ...the swedish-only ESEF filing falls back to its original block, which
    // really is the swedish field...
    expect(html).toContain('data-action="esef:description_sv:Handelsb"');
    // ...and Wikidata's unmarked original is english's field, not swedish's.
    expect(html).toContain('data-action="wikidata:description:Swedish "');
    // The slot sits under the text it acts on.
    expect(html.indexOf("Banking business with deposits and lending.")).
      toBeLessThan(html.indexOf('data-action="scb:'));
  });

  it("renders nothing without proposals", () => {
    const html = renderToStaticMarkup(
      <CompanyDescriptionCard proposals={[]} />,
    );
    expect(html).toBe("");
  });
});
