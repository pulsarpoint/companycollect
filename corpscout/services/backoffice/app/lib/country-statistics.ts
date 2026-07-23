export type StatisticPoint = {
  year: number;
  value: number;
};

export type CountryWorldBankSeries = {
  indicatorCode: string;
  indicatorName: string;
  points: StatisticPoint[];
  latest: StatisticPoint;
};

export type CountryWorldBankStatistics = {
  series: CountryWorldBankSeries[];
  sourceUpdatedDate: string | null;
  pulledAt: string | null;
};

export type CountryImfPoint = StatisticPoint & {
  valueBase: number;
  isEstimate: boolean;
};

export type CountryImfSeries = {
  indicatorCode: string;
  indicatorName: string;
  description: string;
  scale: string | null;
  unit: string | null;
  latestActualYear: number | null;
  points: CountryImfPoint[];
};

export type CountryImfOutlook = {
  vintageDate: string | null;
  series: CountryImfSeries[];
};

export type CountryTradePoint = {
  year: number;
  exportsUsd: number | null;
  importsUsd: number | null;
  balanceUsd: number | null;
  exportsReported: boolean | null;
  importsReported: boolean | null;
};

export type CountryTradeStatistics = {
  points: CountryTradePoint[];
  latest: CountryTradePoint | null;
  pulledAt: string | null;
};

export type EurostatMetricKey =
  | "activeEnterprises"
  | "birthRate"
  | "deathRate"
  | "netGrowthRate"
  | "oneYearSurvivalRate"
  | "highGrowthShare";

export type CountryEurostatMetric = {
  key: EurostatMetricKey;
  label: string;
  value: number;
  year: number;
  status: string;
  unit: "count" | "percent";
};

export type CountryEurostatSizeRow = {
  sizeCode: string;
  label: string;
  enterprises: number | null;
  employment: number | null;
  turnoverEur: number | null;
  valueAddedEur: number | null;
  enterprisesYear: number | null;
  employmentYear: number | null;
  turnoverYear: number | null;
  valueAddedYear: number | null;
};

export type CountryEurostatBusinessStats = {
  coverage: "none" | "partial" | "full";
  datasetCount: number;
  metrics: CountryEurostatMetric[];
  sizeRows: CountryEurostatSizeRow[];
  latestYear: number | null;
};

export const WORLD_BANK_INDICATORS = {
  gdp: "NY.GDP.MKTP.CD",
  gdpPerCapita: "NY.GDP.PCAP.CD",
  realGdpGrowth: "NY.GDP.MKTP.KD.ZG",
  inflation: "FP.CPI.TOTL.ZG",
  unemployment: "SL.UEM.TOTL.ZS",
  population: "SP.POP.TOTL",
  exports: "NE.EXP.GNFS.CD",
  imports: "NE.IMP.GNFS.CD",
} as const;

export const IMF_INDICATORS = {
  realGdpGrowth: "NGDP_RPCH",
  inflation: "PCPIPCH",
  unemployment: "LUR",
  publicDebt: "GGXWDG_NGDP",
  currentAccount: "BCA_NGDP",
  nominalGdp: "NGDPD",
} as const;
