/**
 * The year a country overview is showing, held in the URL.
 *
 * Every card on that page answers a question about a period — what the economy
 * did, what was traded, what companies reported — and they used to answer it
 * about different periods at once: World Bank's latest, Comtrade's latest, and
 * whatever fiscal year each company last filed. Picking one year makes them
 * comparable, and putting it in the URL makes that view linkable.
 *
 * Client-safe: the chart sets it, so no `.server` import.
 */

/** Reject anything outside this: a country page is not a time machine. */
const MIN_YEAR = 1960;
const MAX_YEAR = 2100;

export function parseYear(raw: string | null): number | null {
  if (raw === null) return null;
  const value = Number(raw.trim());
  if (!Number.isInteger(value) || value < MIN_YEAR || value > MAX_YEAR) return null;
  return value;
}

/**
 * The year to show: the requested one when it exists, otherwise the latest.
 *
 * Clamped to years the data actually has rather than honoured blindly — a
 * hand-edited `?year=1975` should land on something real instead of rendering
 * a page of empty cards, and the latest year is what a reader wants by default.
 */
export function resolveYear(
  requested: number | null,
  available: number[],
  fallback?: number | null,
): number | null {
  if (available.length === 0) return null;
  const sorted = [...new Set(available)].sort((a, b) => a - b);
  if (requested !== null && sorted.includes(requested)) return requested;
  // The caller's default when it has one, because "latest" is not always what
  // a reader wants: Sweden's newest fiscal year holds 10,789 filings against
  // the previous year's 386,371, so defaulting to it would show a real year
  // that looks like a collapse. It stays selectable; it is just not the
  // landing view.
  if (fallback != null && sorted.includes(fallback)) return fallback;
  return sorted[sorted.length - 1];
}

/** The query string for a year, empty when it is the default (the latest). */
export function serializeYear(year: number | null, available: number[]): string {
  if (year === null || available.length === 0) return "";
  const latest = Math.max(...available);
  return year === latest ? "" : `year=${year}`;
}
