export type FinancialDemoLocale = "en" | "sv";

export const DEMO_SEK_TO_USD_RATE = 0.1;

const localeCodes: Record<FinancialDemoLocale, string> = {
  en: "en-US",
  sv: "sv-SE",
};

function formatter(
  locale: FinancialDemoLocale,
  options: Intl.NumberFormatOptions,
) {
  return new Intl.NumberFormat(localeCodes[locale], options);
}

export function formatNumber(
  value: number,
  locale: FinancialDemoLocale,
): string {
  return formatter(locale, { maximumFractionDigits: 0 }).format(value);
}

export function formatDecimal(
  value: number,
  locale: FinancialDemoLocale,
): string {
  return formatter(locale, {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  }).format(value);
}

export function formatMoneyPair(
  valueSek: number,
  locale: FinancialDemoLocale,
): { sek: string; usd: string } {
  return {
    sek: `${formatNumber(valueSek, locale)} SEK`,
    usd: `${formatNumber(valueSek * DEMO_SEK_TO_USD_RATE, locale)} USD`,
  };
}

export function formatCompactMoneyPair(
  valueSek: number,
  locale: FinancialDemoLocale,
): string {
  const compact = formatter(locale, {
    notation: "compact",
    maximumFractionDigits: 1,
  });
  return `${compact.format(valueSek)} / ${compact.format(valueSek * DEMO_SEK_TO_USD_RATE)}`;
}

export function formatPercentage(
  value: number,
  locale: FinancialDemoLocale,
): string {
  return `${formatDecimal(value, locale)}%`;
}

export function formatSignedPercentage(
  value: number,
  locale: FinancialDemoLocale,
): string {
  const sign = value > 0 ? "+" : value < 0 ? "−" : "";
  return `${sign}${formatDecimal(Math.abs(value), locale)}%`;
}

export function formatSignedPoints(
  value: number,
  locale: FinancialDemoLocale,
  unit: string,
): string {
  const sign = value > 0 ? "+" : value < 0 ? "−" : "";
  return `${sign}${formatDecimal(Math.abs(value), locale)} ${unit}`;
}
