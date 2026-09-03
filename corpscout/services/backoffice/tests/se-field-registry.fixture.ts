import type {
  FieldRegistry,
  FieldRegistryEntry,
} from "~/lib/se-company-field-registry.server";

/**
 * A registry export as loadFieldRegistry() returns it, small enough to read
 * in one glance. Field names and source orders follow spec section 4.2;
 * `website` is marked python_only here ONLY so the skip path has a case --
 * the real registry marks nothing python_only today. The resolve SQL is a
 * stand-in that names the four parameters every generated statement binds.
 */
export const REGISTRY_VERSION = "se-info-v1";

export function registryEntry(
  over: Partial<FieldRegistryEntry> &
    Pick<FieldRegistryEntry, "field" | "sources">,
): FieldRegistryEntry {
  return {
    valueType: "text",
    displayGroup: "activity",
    structured: false,
    pythonOnly: false,
    policyName: "source_precedence",
    policyVersion: "source_precedence-v1",
    resolveSql: `INSERT INTO corpscout.se_company_field /* ${over.field} */ SELECT {field:String}, {company_ids:Array(String)}, {source_run_id:String}, {resolved_at:DateTime64(3, 'UTC')}`,
    registryVersion: REGISTRY_VERSION,
    ...over,
  };
}

export const REGISTRY_FIXTURE: FieldRegistry = {
  version: REGISTRY_VERSION,
  fields: [
    registryEntry({
      field: "description",
      sources: ["llm", "esef", "wikidata", "scb"],
    }),
    registryEntry({ field: "description_sv", sources: ["llm", "scb"] }),
    registryEntry({
      field: "legal_name",
      displayGroup: "identity",
      sources: ["bolagsverket", "scb", "wikidata"],
    }),
    registryEntry({
      field: "website",
      displayGroup: "scale",
      valueType: "url",
      sources: ["domains", "wikidata"],
      pythonOnly: true,
    }),
  ],
  projectionSql:
    "INSERT INTO corpscout.se_company_info /* projection */ SELECT {company_ids:Array(String)}",
};
