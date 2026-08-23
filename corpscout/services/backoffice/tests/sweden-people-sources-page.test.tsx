import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import { SwedenPeopleSourcesCatalog } from "~/routes/admin-se-people-sources";

describe("SwedenPeopleSourcesCatalog", () => {
  it("owns the country-specific source mapping action", () => {
    const html = renderToStaticMarkup(
      <MemoryRouter>
        <SwedenPeopleSourcesCatalog sourceRoles={[]} canonicalRoles={[]} />
      </MemoryRouter>,
    );

    expect(html).toContain("People sources");
    expect(html).toContain("Bolagsverket");
    expect(html).toContain("ESEF");
    expect(html).toContain("Wikidata");
    expect(html).toContain("Source mappings");
  });
});
