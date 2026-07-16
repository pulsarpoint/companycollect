const ACRONYMS = new Set(["id", "usd", "url", "vat", "eu", "fx", "cnpj", "cvm", "nace", "sni", "ico", "siren"]);

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

export function isLineageKey(key: string): boolean {
  if (key === "source_url") return false;
  if (key.startsWith("source_")) return true;
  return LINEAGE_EXACT.has(key);
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

export function FieldGrid({ fields }: { fields: [string, unknown][] }) {
  if (fields.length === 0) return null;
  return (
    <dl className="grid grid-cols-1 gap-x-8 gap-y-3 sm:grid-cols-2">
      {fields.map(([key, value]) => {
        const formatted = formatFieldValue(key, value);
        const isLink =
          typeof formatted === "string" &&
          (formatted.startsWith("http://") || formatted.startsWith("https://"));
        return (
          <div key={key} className="flex flex-col gap-0.5">
            <dt className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
              {humanizeFieldKey(key)}
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
