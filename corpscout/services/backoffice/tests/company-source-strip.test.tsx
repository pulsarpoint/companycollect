import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  companySourceLabel,
  companySourceLabels,
  CompanySourceStrip,
} from "~/components/admin/company-source-strip";
import { PROFILE_SOURCES } from "~/lib/se-company-info-filters";

describe("companySourceLabel", () => {
  it("names every source of the profile catalog exactly as the catalog does", () => {
    // The strip and the list page's letters must not spell one register two
    // ways: this is the same lookup the legend and the filter chip use.
    for (const source of PROFILE_SOURCES) {
      expect(companySourceLabel(source.value)).toBe(source.label);
    }
  });

  it("folds a datatype's own spelling of a register onto the catalog's name", () => {
    // The Financial tab calls Bolagsverket 'bolagsverket-annual-accounts' and
    // the Domains tab calls ESEF 'esef_filing'. One register, one name.
    expect(companySourceLabel("bolagsverket-annual-accounts")).toBe("Bolagsverket");
    expect(companySourceLabel("esef_filing")).toBe("ESEF");
    // ...and a suggester that is not a register still reads as prose.
    expect(companySourceLabel("common_crawl_identity")).toBe("Common Crawl");
  });

  it("shows a token it cannot name rather than dropping it", () => {
    // A source this bundle predates is still data about the company.
    expect(companySourceLabel("some_new_register")).toBe("some_new_register");
  });
});

describe("companySourceLabels", () => {
  it("orders the registers the way the list page's letters are ordered", () => {
    expect(
      companySourceLabels(["wikidata", "esef", "scb", "bolagsverket"]),
    ).toEqual(["Bolagsverket", "SCB", "ESEF", "Wikidata"]);
    // ...whatever order ClickHouse handed them over in.
    expect(
      companySourceLabels(["scb", "wikidata", "bolagsverket", "esef"]),
    ).toEqual(["Bolagsverket", "SCB", "ESEF", "Wikidata"]);
  });

  it("dedupes by NAME, so two spellings of one register say it once", () => {
    expect(companySourceLabels(["esef", "esef_filing", "esef"])).toEqual(["ESEF"]);
    expect(
      companySourceLabels(["bolagsverket-annual-accounts", "bolagsverket"]),
    ).toEqual(["Bolagsverket"]);
  });

  it("sorts what the catalog does not name after everything it does, alphabetically", () => {
    expect(
      companySourceLabels(["some_new_register", "common_crawl_identity", "scb"]),
    ).toEqual(["SCB", "Common Crawl", "some_new_register"]);
  });

  it("has nothing to say about no sources", () => {
    expect(companySourceLabels([])).toEqual([]);
  });
});

describe("CompanySourceStrip", () => {
  it("names each register in full -- the detail pages have room, the list page has letters", () => {
    const html = renderToStaticMarkup(
      <CompanySourceStrip sources={["scb", "bolagsverket"]} />,
    );
    expect(html).toContain("Sources");
    expect(html).toContain(">Bolagsverket<");
    expect(html).toContain(">SCB<");
    // Never the list column's letters: a one-letter badge here would read as
    // a different fact from the one beside it.
    expect(html).not.toContain(">B<");
    expect(html).not.toContain(">S<");
    // The rendered order is the catalog's, whatever order it was handed.
    expect(html.indexOf(">Bolagsverket<")).toBeLessThan(html.indexOf(">SCB<"));
    expect(html).toContain('data-source-strip="Bolagsverket,SCB"');
  });

  it("says \"nothing\" with the shared em dash rather than a blank line", () => {
    const html = renderToStaticMarkup(<CompanySourceStrip sources={[]} />);
    expect(html).toContain("Sources");
    expect(html).toContain("—");
    expect(html).toContain('data-source-strip=""');
  });
});
