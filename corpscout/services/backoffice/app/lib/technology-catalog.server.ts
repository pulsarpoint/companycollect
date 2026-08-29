import { chQuery } from "~/lib/clickhouse.server";

/**
 * Lookups against `corpscout.technology_catalog` (7.9k rows, one per exact
 * detector name). The table is a ReplacingMergeTree keyed by `technology`, so
 * every query reads FINAL. Icons live in the object store and are only ever
 * served through the /icons/tech/:slug resource route — nothing here leaks
 * bucket names or endpoints to the browser.
 */

/** One catalog row, projected down to what the technology pages render. */
export interface TechnologyCatalogEntry {
  slug: string;
  description: string;
  website: string;
  categories: string[];
  saas: boolean;
  oss: boolean;
  /** True when the catalog stores an icon, served at /icons/tech/:slug. */
  icon: boolean;
}

interface TechnologyCatalogRow {
  technology: string;
  slug: string;
  description: string;
  website: string;
  categories: string[];
  has_icon: 0 | 1;
  saas: 0 | 1;
  oss: 0 | 1;
}

/** A page shows at most a few dozen technologies; this is a hard safety cap. */
const MAX_CATALOG_NAMES = 500;

export const TECHNOLOGY_CATALOG_ENTRIES_SQL = `SELECT
  technology,
  slug,
  description,
  website,
  categories,
  toUInt8(icon_object_key != '') AS has_icon,
  saas,
  oss
FROM corpscout.technology_catalog FINAL
WHERE technology IN {names:Array(String)}
LIMIT ${MAX_CATALOG_NAMES}`;

/**
 * Batch-resolves detector names to catalog entries in one keyed FINAL query.
 * Names are deduplicated and capped; unknown names are simply absent from the
 * result. Returns a plain object so loader data serializes as-is.
 */
export async function loadTechnologyCatalogEntries(
  names: string[],
): Promise<Record<string, TechnologyCatalogEntry>> {
  const distinct = Array.from(new Set(names.filter(Boolean))).slice(
    0,
    MAX_CATALOG_NAMES,
  );
  if (distinct.length === 0) return {};
  const rows = await chQuery<TechnologyCatalogRow>(
    TECHNOLOGY_CATALOG_ENTRIES_SQL,
    { names: distinct },
  );
  const entries: Record<string, TechnologyCatalogEntry> = {};
  for (const row of rows) {
    entries[row.technology] = {
      slug: row.slug,
      description: row.description,
      website: row.website,
      categories: row.categories,
      saas: Boolean(row.saas),
      oss: Boolean(row.oss),
      icon: Boolean(row.has_icon),
    };
  }
  return entries;
}

/** Where a technology's icon lives, plus what the ETag should be built from. */
export interface TechnologyIconRef {
  objectKey: string;
  contentType: string;
  updatedAt: string;
}

interface TechnologyIconRow {
  icon_object_key: string;
  icon_content_type: string;
  updated_at: string;
}

export const TECHNOLOGY_ICON_SQL = `SELECT
  icon_object_key,
  icon_content_type,
  toString(updated_at) AS updated_at
FROM corpscout.technology_catalog FINAL
WHERE slug = {slug:String}
LIMIT 1`;

/**
 * Resolves an icon slug to its object-store location. Null when the slug is
 * unknown or the catalog row has no icon.
 */
export async function loadTechnologyIconRef(
  slug: string,
): Promise<TechnologyIconRef | null> {
  const rows = await chQuery<TechnologyIconRow>(TECHNOLOGY_ICON_SQL, { slug });
  const row = rows[0];
  if (!row || row.icon_object_key === "") return null;
  return {
    objectKey: row.icon_object_key,
    contentType: row.icon_content_type,
    updatedAt: row.updated_at,
  };
}
