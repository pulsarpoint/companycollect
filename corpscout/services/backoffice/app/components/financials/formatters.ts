import type { FinancialLocale } from "~/components/financials/copy";

export type MoneyPair = {
  original: number | null | undefined;
  usd: number | null | undefined;
  currency: string;
};

const localeCodes: Record<FinancialLocale, string> = {
  en: "en-US",
  sv: "sv-SE",
};

function formatter(locale: FinancialLocale, options: Intl.NumberFormatOptions) {
  return new Intl.NumberFormat(localeCodes[locale], options);
}

export function formatNumber(value: number, locale: FinancialLocale): string {
  return formatter(locale, { maximumFractionDigits: 0 }).format(value);
}

export function formatDecimal(value: number, locale: FinancialLocale): string {
  return formatter(locale, {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  }).format(value);
}

export function formatPercentage(
  value: number,
  locale: FinancialLocale,
): string {
  return `${formatDecimal(value, locale)}%`;
}

export function formatSignedPercentage(
  value: number,
  locale: FinancialLocale,
): string {
  const sign = value > 0 ? "+" : value < 0 ? "−" : "";
  return `${sign}${formatDecimal(Math.abs(value), locale)}%`;
}

export function formatSignedPoints(
  value: number,
  locale: FinancialLocale,
  unit: string,
): string {
  const sign = value > 0 ? "+" : value < 0 ? "−" : "";
  return `${sign}${formatDecimal(Math.abs(value), locale)} ${unit}`;
}

export function formatCompactMoney(
  value: number,
  locale: FinancialLocale,
): string {
  return formatter(locale, {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

export function formatDate(value: string, locale: FinancialLocale): string {
  const date = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(date.valueOf())) return value;
  return new Intl.DateTimeFormat(localeCodes[locale], {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  }).format(date);
}

export function moneyPairLines(
  pair: MoneyPair,
  locale: FinancialLocale,
): { original: string | null; usd: string | null } {
  return {
    original:
      pair.original == null
        ? null
        : `${formatNumber(pair.original, locale)} ${pair.currency || "SEK"}`,
    usd: pair.usd == null ? null : `${formatNumber(pair.usd, locale)} USD`,
  };
}
