const ACRONYMS = new Set(["id", "usd", "url", "vat", "eu", "fx", "cnpj", "cvm", "nace", "sni", "ico", "siren", "lei"]);

export function humanizeFieldKey(key: string): string {
  const words = key.split("_").map((w) => (ACRONYMS.has(w) ? w.toUpperCase() : w));
  const joined = words.join(" ");
  return joined.charAt(0).toUpperCase() + joined.slice(1);
}

const FLAG_PREFIXES = ["is_", "has_", "opted_"];
const nf = new Intl.NumberFormat("en-US");

export function formatFieldValue(key: string, value: unknown): string | null {
  if (value == null || value === "") return null;
  if (
    FLAG_PREFIXES.some((p) => key.startsWith(p)) &&
    (value === 0 || value === 1 || value === "0" || value === "1")
  ) {
    return Number(value) === 1 ? "yes" : "no";
  }
  if (typeof value === "number" && Number.isFinite(value)) return nf.format(value);
  return String(value);
}

const LINEAGE_EXACT = new Set([
  "country_iso2", "source_system", "resolved_at", "updated_from_raw_at",
  "name_normalized", "xml_object_key", "xml_sha256", "xml_size_bytes",
]);

// Translation-lineage suffixes: `<base>_language` records which language a
// paired field was translated from, and the `_translated_at`/`_translation_*`
// trio records how the translation was produced. Neither is content — both
// belong in the lineage bucket alongside `source_*`.
// `_raw` fields are verbatim source values kept for provenance (e.g. SE
// legal_name_raw is Bolagsverket's packed multi-name string) — every one has
// a parsed counterpart in the main grid, so readers never need them inline.
const LINEAGE_SUFFIXES = [
  "_language", "_translated_at", "_translation_provider", "_translation_model",
  "_raw", "_payload_hash", "_source_record_id", "_sha256",
];

export function isLineageKey(key: string): boolean {
  if (key === "source_url") return false;
  if (key.startsWith("source_")) return true;
  if (LINEAGE_EXACT.has(key)) return true;
  return LINEAGE_SUFFIXES.some((suffix) => key.endsWith(suffix));
}

export function splitFields(record: Record<string, unknown>): {
  visible: [string, unknown][];
  lineage: [string, unknown][];
} {
  const visible: [string, unknown][] = [];
  const lineage: [string, unknown][] = [];
  for (const [key, value] of Object.entries(record)) {
    (isLineageKey(key) ? lineage : visible).push([key, value]);
  }
  return { visible, lineage };
}

const EMPTY = <span className="text-muted-foreground">—</span>;

export function FieldGrid({
  fields,
  markers,
}: {
  fields: [string, unknown][];
  /**
   * Optional per-key muted suffix text (e.g. "(original)"/"(english)"),
   * rendered next to a field's label — mirrors the fallback marker
   * ProseSections shows for long-text fields, extended to the grid. Callers
   * that don't need markers omit the prop entirely; existing call sites are
   * unaffected.
   */
  markers?: Map<string, string>;
}) {
  if (fields.length === 0) return null;
  return (
    <dl className="grid grid-cols-1 gap-x-8 gap-y-3 sm:grid-cols-2">
      {fields.map(([key, value]) => {
        const formatted = formatFieldValue(key, value);
        const isLink =
          typeof formatted === "string" &&
          (formatted.startsWith("http://") || formatted.startsWith("https://"));
        const marker = markers?.get(key);
        return (
          <div key={key} className="flex flex-col gap-0.5">
            <dt className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
              {humanizeFieldKey(key)}
              {marker ? (
                <span className="text-muted-foreground/70 ml-1.5 font-normal normal-case">
                  {marker}
                </span>
              ) : null}
            </dt>
            <dd className="text-sm break-words tabular-nums">
              {formatted === null ? (
                EMPTY
              ) : isLink ? (
                <a href={formatted} target="_blank" rel="noreferrer" className="underline underline-offset-2">
                  {formatted}
                </a>
              ) : (
                formatted
              )}
            </dd>
          </div>
        );
      })}
    </dl>
  );
}
