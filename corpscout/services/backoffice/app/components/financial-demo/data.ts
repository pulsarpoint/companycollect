export type FinancialDemoYear = 2021 | 2022 | 2023 | 2024 | 2025;

export type FinancialDemoPoint = {
  year: FinancialDemoYear;
  periodStart: string;
  periodEnd: string;
  revenue: number;
  otherOperatingIncome: number;
  operatingIncome: number;
  operatingExpenses: number;
  operatingResult: number;
  profitAfterFinancialItems: number;
  tax: number;
  netResult: number;
  fixedAssets: number;
  currentAssets: number;
  cashAndBank: number;
  currentReceivables: number;
  totalAssets: number;
  equity: number;
  shareCapital: number;
  untaxedReserves: number;
  longTermLiabilities: number;
  currentLiabilities: number;
  personnelExpenses: number;
  employees: number;
  proposedDividend: number;
};

/**
 * Static prototype data for 1404 Reklam AB. Values mirror a subset of the
 * current Swedish filing facts so the design can be judged with realistic
 * magnitudes, but this module is deliberately disconnected from ClickHouse.
 */
export const financialDemoData: FinancialDemoPoint[] = [
  {
    year: 2021,
    periodStart: "2021-01-01",
    periodEnd: "2021-12-31",
    revenue: 675_703,
    otherOperatingIncome: 0,
    operatingIncome: 675_703,
    operatingExpenses: -711_399,
    operatingResult: -35_696,
    profitAfterFinancialItems: -36_156,
    tax: -3_357,
    netResult: 5_487,
    fixedAssets: 0,
    currentAssets: 271_172,
    cashAndBank: 191_995,
    currentReceivables: 79_177,
    totalAssets: 271_172,
    equity: 198_187,
    shareCapital: 50_000,
    untaxedReserves: 0,
    longTermLiabilities: 0,
    currentLiabilities: 72_985,
    personnelExpenses: 503_364,
    employees: 1,
    proposedDividend: 80_000,
  },
  {
    year: 2022,
    periodStart: "2022-01-01",
    periodEnd: "2022-12-31",
    revenue: 886_078,
    otherOperatingIncome: 1_351,
    operatingIncome: 887_429,
    operatingExpenses: -722_729,
    operatingResult: 164_700,
    profitAfterFinancialItems: 164_613,
    tax: -36_855,
    netResult: 127_758,
    fixedAssets: 0,
    currentAssets: 426_741,
    cashAndBank: 186_540,
    currentReceivables: 240_201,
    totalAssets: 426_741,
    equity: 325_945,
    shareCapital: 50_000,
    untaxedReserves: 0,
    longTermLiabilities: 0,
    currentLiabilities: 100_796,
    personnelExpenses: 479_087,
    employees: 1,
    proposedDividend: 100_000,
  },
  {
    year: 2023,
    periodStart: "2023-01-01",
    periodEnd: "2023-12-31",
    revenue: 644_789,
    otherOperatingIncome: 22_168,
    operatingIncome: 666_957,
    operatingExpenses: -574_074,
    operatingResult: 92_883,
    profitAfterFinancialItems: 92_821,
    tax: -23_275,
    netResult: 69_546,
    fixedAssets: 0,
    currentAssets: 344_949,
    cashAndBank: 241_397,
    currentReceivables: 103_552,
    totalAssets: 344_949,
    equity: 295_490,
    shareCapital: 50_000,
    untaxedReserves: 0,
    longTermLiabilities: 0,
    currentLiabilities: 49_459,
    personnelExpenses: 404_234,
    employees: 1,
    proposedDividend: 100_000,
  },
  {
    year: 2024,
    periodStart: "2024-01-01",
    periodEnd: "2024-12-31",
    revenue: 727_888,
    otherOperatingIncome: 5_906,
    operatingIncome: 733_794,
    operatingExpenses: -670_464,
    operatingResult: 63_330,
    profitAfterFinancialItems: 63_287,
    tax: -15_299,
    netResult: 47_988,
    fixedAssets: 0,
    currentAssets: 301_709,
    cashAndBank: 158_540,
    currentReceivables: 143_169,
    totalAssets: 301_709,
    equity: 243_478,
    shareCapital: 50_000,
    untaxedReserves: 0,
    longTermLiabilities: 0,
    currentLiabilities: 58_231,
    personnelExpenses: 503_552,
    employees: 1,
    proposedDividend: 60_000,
  },
  {
    year: 2025,
    periodStart: "2025-01-01",
    periodEnd: "2025-12-31",
    revenue: 684_527,
    otherOperatingIncome: 5_000,
    operatingIncome: 689_527,
    operatingExpenses: -635_372,
    operatingResult: 54_155,
    profitAfterFinancialItems: 54_387,
    tax: -12_512,
    netResult: 41_875,
    fixedAssets: 0,
    currentAssets: 284_033,
    cashAndBank: 152_437,
    currentReceivables: 131_596,
    totalAssets: 284_033,
    equity: 225_353,
    shareCapital: 50_000,
    untaxedReserves: 0,
    longTermLiabilities: 0,
    currentLiabilities: 58_680,
    personnelExpenses: 504_599,
    employees: 1,
    proposedDividend: 70_000,
  },
];

export const latestFinancialDemoPoint = financialDemoData.at(-1)!;
export const previousFinancialDemoPoint = financialDemoData.at(-2)!;
