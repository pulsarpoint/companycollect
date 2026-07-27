import { describe, expect, it } from "vitest";
import { sourceSlugToPath } from "./procurements.server";

describe("sourceSlugToPath", () => {
  it("drops the _procurement suffix and hyphenates", () => {
    expect(sourceSlugToPath("ted_procurement")).toBe("ted");
    expect(sourceSlugToPath("sweden_uhm_procurement")).toBe("sweden-uhm");
    expect(sourceSlugToPath("finland_hilma_procurement")).toBe("finland-hilma");
    expect(sourceSlugToPath("norway_doffin_procurement")).toBe("norway-doffin");
    expect(sourceSlugToPath("brazil_pncp_procurement")).toBe("brazil-pncp");
  });

  it("produces a distinct path per source", () => {
    const slugs = [
      "ted_procurement",
      "sweden_uhm_procurement",
      "finland_hilma_procurement",
      "norway_doffin_procurement",
      "brazil_pncp_procurement",
    ];
    const paths = slugs.map(sourceSlugToPath);
    expect(new Set(paths).size).toBe(slugs.length);
  });

  it("is derived rather than stored, so it cannot drift from the slug", () => {
    // A source added later gets a path for free, with no second place to edit.
    expect(sourceSlugToPath("denmark_udbud_procurement")).toBe("denmark-udbud");
  });
});
