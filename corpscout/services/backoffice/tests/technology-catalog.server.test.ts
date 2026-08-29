import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  chQuery: vi.fn(),
}));

vi.mock("~/lib/clickhouse.server", () => ({ chQuery: mocks.chQuery }));

const {
  TECHNOLOGY_CATALOG_ENTRIES_SQL,
  TECHNOLOGY_ICON_SQL,
  loadTechnologyCatalogEntries,
  loadTechnologyIconRef,
} = await import("~/lib/technology-catalog.server");

beforeEach(() => {
  mocks.chQuery.mockReset();
});

describe("technology catalog SQL shape", () => {
  it("reads the ReplacingMergeTree FINAL, binds names as Array(String), caps rows", () => {
    expect(TECHNOLOGY_CATALOG_ENTRIES_SQL).toContain(
      "FROM corpscout.technology_catalog FINAL",
    );
    expect(TECHNOLOGY_CATALOG_ENTRIES_SQL).toContain(
      "WHERE technology IN {names:Array(String)}",
    );
    expect(TECHNOLOGY_CATALOG_ENTRIES_SQL).toMatch(/LIMIT \d+/);
  });

  it("keys the icon lookup by slug against FINAL with LIMIT 1", () => {
    expect(TECHNOLOGY_ICON_SQL).toContain(
      "FROM corpscout.technology_catalog FINAL",
    );
    expect(TECHNOLOGY_ICON_SQL).toContain("WHERE slug = {slug:String}");
    expect(TECHNOLOGY_ICON_SQL).toContain("LIMIT 1");
  });
});

describe("loadTechnologyCatalogEntries", () => {
  it("returns an empty map without querying when there are no names", async () => {
    await expect(loadTechnologyCatalogEntries([])).resolves.toEqual({});
    await expect(loadTechnologyCatalogEntries(["", ""])).resolves.toEqual({});
    expect(mocks.chQuery).not.toHaveBeenCalled();
  });

  it("deduplicates names and maps rows keyed by exact detector name", async () => {
    mocks.chQuery.mockResolvedValueOnce([
      {
        technology: "WordPress",
        slug: "wordpress",
        description: "WordPress is a content management system.",
        website: "https://wordpress.org",
        categories: ["CMS", "Blogs"],
        has_icon: 1,
        saas: 0,
        oss: 1,
      },
      {
        technology: "jQuery",
        slug: "jquery",
        description: "jQuery is a JavaScript library.",
        website: "https://jquery.com",
        categories: ["JavaScript libraries"],
        has_icon: 0,
        saas: 0,
        oss: 1,
      },
    ]);

    const entries = await loadTechnologyCatalogEntries([
      "WordPress",
      "jQuery",
      "WordPress",
      "Unknown Tech",
    ]);

    expect(mocks.chQuery).toHaveBeenCalledWith(
      TECHNOLOGY_CATALOG_ENTRIES_SQL,
      { names: ["WordPress", "jQuery", "Unknown Tech"] },
    );
    expect(entries.WordPress).toEqual({
      slug: "wordpress",
      description: "WordPress is a content management system.",
      website: "https://wordpress.org",
      categories: ["CMS", "Blogs"],
      saas: false,
      oss: true,
      icon: true,
    });
    expect(entries.jQuery.icon).toBe(false);
    expect(entries["Unknown Tech"]).toBeUndefined();
  });
});

describe("loadTechnologyIconRef", () => {
  it("resolves a slug to its object key, content type, and version stamp", async () => {
    mocks.chQuery.mockResolvedValueOnce([
      {
        icon_object_key: "icons/wordpress.svg",
        icon_content_type: "image/svg+xml",
        updated_at: "2026-08-28 10:00:00",
      },
    ]);
    await expect(loadTechnologyIconRef("wordpress")).resolves.toEqual({
      objectKey: "icons/wordpress.svg",
      contentType: "image/svg+xml",
      updatedAt: "2026-08-28 10:00:00",
    });
    expect(mocks.chQuery).toHaveBeenCalledWith(TECHNOLOGY_ICON_SQL, {
      slug: "wordpress",
    });
  });

  it("returns null for unknown slugs and for rows without an icon", async () => {
    mocks.chQuery.mockResolvedValueOnce([]);
    await expect(loadTechnologyIconRef("nope")).resolves.toBeNull();

    mocks.chQuery.mockResolvedValueOnce([
      {
        icon_object_key: "",
        icon_content_type: "",
        updated_at: "2026-08-28 10:00:00",
      },
    ]);
    await expect(loadTechnologyIconRef("iconless")).resolves.toBeNull();
  });
});
