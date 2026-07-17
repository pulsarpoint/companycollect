const METHODOLOGY_COPY =
  "Latest filed year per company, converted to USD at period-end rates. Standalone (non-consolidated) accounts; totals may double-count corporate groups. Norway excludes foreign-branch (NUF) filings from sums.";

export function MethodologyNote() {
  return <p className="text-muted-foreground text-xs">{METHODOLOGY_COPY}</p>;
}
