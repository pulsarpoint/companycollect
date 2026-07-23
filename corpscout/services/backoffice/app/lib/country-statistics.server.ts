import { chQuery } from "~/lib/clickhouse.server";
import type { CountryConfig } from "~/lib/countries";
import {
  IMF_INDICATORS,
  WORLD_BANK_INDICATORS,
  type CountryEurostatBusinessStats,
  type CountryEurostatMetric,
  type CountryEurostatSizeRow,
  type CountryImfOutlook,
  type CountryImfSeries,
  type CountryTradeStatistics,
  type CountryWorldBankStatistics,
  type EurostatMetricKey,
  type StatisticPoint,
} from "~/lib/country-statistics";

type WorldBankRow = {
  indicator_code: string;
  indicator_name: string;
  year: number | string;
  indicator_value: number | string;
  latest_source_updated_date: string;
  latest_pulled_at: string;
};

export async function getCountryWorldBankStatistics(
  countryCode: string,
): Promise<CountryWorldBankStatistics> {
  const rows = await chQuery<WorldBankRow>(
    `
      SELECT
        indicator_code,
        any(indicator_name) AS indicator_name,
        year,
        argMax(value, pulled_at) AS indicator_value,
        toString(max(source_updated_date)) AS latest_source_updated_date,
        toString(max(pulled_at)) AS latest_pulled_at
      FROM world_bank_macro_observations
      WHERE country_code = {country:String}
        AND indicator_code IN {indicators:Array(String)}
      GROUP BY indicator_code, year
      ORDER BY indicator_code, year
    `,
    {
      country: countryCode.toLowerCase(),
      indicators: Object.values(WORLD_BANK_INDICATORS),
    },
  );

  const pointsByIndicator = new Map<string, StatisticPoint[]>();
  const namesByIndicator = new Map<string, string>();
  let sourceUpdatedDate: string | null = null;
  let pulledAt: string | null = null;

  for (const row of rows) {
    const year = Number(row.year);
    const value = Number(row.indicator_value);
    if (!Number.isInteger(year) || !Number.isFinite(value)) continue;

    const points = pointsByIndicator.get(row.indicator_code) ?? [];
    points.push({ year, value });
    pointsByIndicator.set(row.indicator_code, points);
    namesByIndicator.set(row.indicator_code, row.indicator_name);
    if (row.latest_source_updated_date > (sourceUpdatedDate ?? "")) {
      sourceUpdatedDate = row.latest_source_updated_date;
    }
    if (row.latest_pulled_at > (pulledAt ?? "")) pulledAt = row.latest_pulled_at;
  }

  const series = Object.values(WORLD_BANK_INDICATORS).flatMap((indicatorCode) => {
    const points = pointsByIndicator.get(indicatorCode);
    if (!points?.length) return [];
    return [{
      indicatorCode,
      indicatorName: namesByIndicator.get(indicatorCode) ?? indicatorCode,
      points,
      latest: points.at(-1)!,
    }];
  });

  return { series, sourceUpdatedDate, pulledAt };
}

type ImfRow = {
  vintage_date: string;
  indicator_code: string;
  indicator_name: string;
  indicator_description: string;
  scale: string | null;
  unit: string | null;
  latest_actual_year: number | string | null;
  year: number | string;
  value: number | string;
  value_base: number | string;
  is_estimate: boolean | number;
};

export async function getCountryImfOutlook(countryIso3: string): Promise<CountryImfOutlook> {
  const rows = await chQuery<ImfRow>(
    `
      WITH (SELECT max(vintage_date) FROM imf_weo_observations) AS latest_vintage
      SELECT
        toString(o.vintage_date) AS vintage_date,
        o.indicator_code,
        s.indicator_name,
        s.indicator_description,
        s.scale,
        s.unit,
        s.latest_actual_year,
        o.year,
        o.value,
        o.value_base,
        o.is_estimate
      FROM imf_weo_observations AS o
      INNER JOIN imf_weo_series AS s
        ON s.vintage_date = o.vintage_date
        AND s.country_iso3 = o.country_iso3
        AND s.indicator_code = o.indicator_code
      WHERE o.vintage_date = latest_vintage
        AND o.country_iso3 = {countryIso3:String}
        AND o.indicator_code IN {indicators:Array(String)}
      ORDER BY o.indicator_code, o.year
    `,
    {
      countryIso3,
      indicators: Object.values(IMF_INDICATORS),
    },
  );

  const seriesByIndicator = new Map<string, CountryImfSeries>();
  for (const row of rows) {
    const year = Number(row.year);
    const value = Number(row.value);
    const valueBase = Number(row.value_base);
    if (!Number.isInteger(year) || !Number.isFinite(value) || !Number.isFinite(valueBase)) {
      continue;
    }

    const series = seriesByIndicator.get(row.indicator_code) ?? {
      indicatorCode: row.indicator_code,
      indicatorName: row.indicator_name,
      description: row.indicator_description,
      scale: row.scale,
      unit: row.unit,
      latestActualYear:
        row.latest_actual_year === null ? null : Number(row.latest_actual_year),
      points: [],
    };
    series.points.push({
      year,
      value,
      valueBase,
      isEstimate: Boolean(row.is_estimate),
    });
    seriesByIndicator.set(row.indicator_code, series);
  }

  return {
    vintageDate: rows[0]?.vintage_date ?? null,
    series: Object.values(IMF_INDICATORS).flatMap((code) => {
      const series = seriesByIndicator.get(code);
      return series ? [series] : [];
    }),
  };
}

type TradeRow = {
  year: number | string;
  exports_usd: number | string | null;
  imports_usd: number | string | null;
  exports_reported: boolean | number | null;
  imports_reported: boolean | number | null;
  latest_pulled_at: string;
};

export async function getCountryTradeStatistics(
  countryIso3: string,
): Promise<CountryTradeStatistics> {
  const rows = await chQuery<TradeRow>(
    `
      SELECT
        year,
        if(
          countIf(flow_code = 'X') > 0,
          toNullable(toFloat64(maxIf(primary_value_usd, flow_code = 'X'))),
          NULL
        ) AS exports_usd,
        if(
          countIf(flow_code = 'M') > 0,
          toNullable(toFloat64(maxIf(primary_value_usd, flow_code = 'M'))),
          NULL
        ) AS imports_usd,
        maxIf(toNullable(is_reported), flow_code = 'X') AS exports_reported,
        maxIf(toNullable(is_reported), flow_code = 'M') AS imports_reported,
        toString(max(pulled_at)) AS latest_pulled_at
      FROM un_comtrade_annual_totals
      WHERE reporter_iso = {countryIso3:String}
        AND flow_code IN ('M', 'X')
      GROUP BY year
      ORDER BY year
    `,
    { countryIso3 },
  );

  let pulledAt: string | null = null;
  const points = rows.map((row) => {
    const exportsUsd = nullableNumber(row.exports_usd);
    const importsUsd = nullableNumber(row.imports_usd);
    if (row.latest_pulled_at > (pulledAt ?? "")) pulledAt = row.latest_pulled_at;
    return {
      year: Number(row.year),
      exportsUsd,
      importsUsd,
      balanceUsd:
        exportsUsd === null || importsUsd === null ? null : exportsUsd - importsUsd,
      exportsReported:
        row.exports_reported === null ? null : Boolean(row.exports_reported),
      importsReported:
        row.imports_reported === null ? null : Boolean(row.imports_reported),
    };
  });

  return { points, latest: points.at(-1) ?? null, pulledAt };
}

const EUROSTAT_METRICS: Array<{
  key: EurostatMetricKey;
  label: string;
  datasetCode: "bd_size" | "bd_hg";
  keyPrefix: string;
  unit: "count" | "percent";
}> = [
  {
    key: "activeEnterprises",
    label: "Active enterprises",
    datasetCode: "bd_size",
    keyPrefix: "A,TOTAL,TOTAL,ENT_NR,B-S_X_O_S94",
    unit: "count",
  },
  {
    key: "birthRate",
    label: "Enterprise birth rate",
    datasetCode: "bd_size",
    keyPrefix: "A,TOTAL,TOTAL,ENT_BRTHR_PC,B-S_X_O_S94",
    unit: "percent",
  },
  {
    key: "deathRate",
    label: "Enterprise death rate",
    datasetCode: "bd_size",
    keyPrefix: "A,TOTAL,TOTAL,ENT_DTHR_PC,B-S_X_O_S94",
    unit: "percent",
  },
  {
    key: "netGrowthRate",
    label: "Net enterprise growth",
    datasetCode: "bd_size",
    keyPrefix: "A,TOTAL,TOTAL,GRW_ENT_PC,B-S_X_O_S94",
    unit: "percent",
  },
  {
    key: "oneYearSurvivalRate",
    label: "One-year survival rate",
    datasetCode: "bd_size",
    keyPrefix: "A,Y1,TOTAL,ENT_SRVLR_BRTH_PC,B-S_X_O_S94",
    unit: "percent",
  },
  {
    key: "highGrowthShare",
    label: "High-growth enterprises",
    datasetCode: "bd_hg",
    keyPrefix: "A,ENT_HGRWR_PC,B-S_X_O_S94",
    unit: "percent",
  },
];

const SIZE_LABELS: Record<string, string> = {
  "0-9": "0–9",
  "10-19": "10–19",
  "20-49": "20–49",
  "50-249": "50–249",
  GE250: "250+",
};

type EurostatCoverageRow = {
  dataset_count: number | string;
  latest_year: number | string | null;
};

type EurostatMetricRow = {
  dataset_code: string;
  series_key: string;
  latest_year: number | string;
  metric_value: number | string;
  status: string;
};

type EurostatSizeRow = {
  size_code: string;
  enterprises: number | string | null;
  employment: number | string | null;
  turnover_eur: number | string | null;
  value_added_eur: number | string | null;
  enterprises_year: number | string | null;
  employment_year: number | string | null;
  turnover_year: number | string | null;
  value_added_year: number | string | null;
};

export async function getCountryEurostatBusinessStats(
  country: Pick<CountryConfig, "eurostatGeoCode">,
): Promise<CountryEurostatBusinessStats> {
  const geoCode = country.eurostatGeoCode;
  if (!geoCode) return emptyEurostatStats();

  const metricKeys = EUROSTAT_METRICS.map(
    (metric) => `${metric.keyPrefix},${geoCode}`,
  );
  const sizeKeys = ["0-9", "10-19", "20-49", "50-249", "GE250"].flatMap(
    (sizeCode) =>
      ["ENT_NR", "EMP_NR", "NETTUR_MEUR", "AV_MEUR"].map(
        (indicator) => `A,${indicator},B-S_X_O_S94,${sizeCode},${geoCode}`,
      ),
  );

  const [coverageRows, metricRows, sizeRows] = await Promise.all([
    chQuery<EurostatCoverageRow>(
      `
        SELECT
          uniqExact(dataset_code) AS dataset_count,
          max(year) AS latest_year
        FROM eurostat_observations
        WHERE geo_code = {geoCode:String}
          AND value IS NOT NULL
      `,
      { geoCode },
    ),
    chQuery<EurostatMetricRow>(
      `
        SELECT
          dataset_code,
          series_key,
          max(year) AS latest_year,
          argMax(value, year) AS metric_value,
          argMax(status, year) AS status
        FROM eurostat_observations
        WHERE geo_code = {geoCode:String}
          AND series_key IN {seriesKeys:Array(String)}
          AND value IS NOT NULL
        GROUP BY dataset_code, series_key
      `,
      { geoCode, seriesKeys: metricKeys },
    ),
    chQuery<EurostatSizeRow>(
      `
        SELECT
          splitByChar(',', series_key)[4] AS size_code,
          argMaxIf(value, year, splitByChar(',', series_key)[2] = 'ENT_NR') AS enterprises,
          argMaxIf(value, year, splitByChar(',', series_key)[2] = 'EMP_NR') AS employment,
          argMaxIf(value, year, splitByChar(',', series_key)[2] = 'NETTUR_MEUR') * 1000000 AS turnover_eur,
          argMaxIf(value, year, splitByChar(',', series_key)[2] = 'AV_MEUR') * 1000000 AS value_added_eur,
          maxIf(year, splitByChar(',', series_key)[2] = 'ENT_NR') AS enterprises_year,
          maxIf(year, splitByChar(',', series_key)[2] = 'EMP_NR') AS employment_year,
          maxIf(year, splitByChar(',', series_key)[2] = 'NETTUR_MEUR') AS turnover_year,
          maxIf(year, splitByChar(',', series_key)[2] = 'AV_MEUR') AS value_added_year
        FROM eurostat_observations
        WHERE dataset_code = 'sbs_sc_ovw'
          AND geo_code = {geoCode:String}
          AND series_key IN {seriesKeys:Array(String)}
          AND value IS NOT NULL
        GROUP BY size_code
      `,
      { geoCode, seriesKeys: sizeKeys },
    ),
  ]);

  const coverageRow = coverageRows[0];
  const datasetCount = Number(coverageRow?.dataset_count ?? 0);
  const metricDefinitionBySeries = new Map(
    EUROSTAT_METRICS.map((metric) => [
      `${metric.keyPrefix},${geoCode}`,
      metric,
    ]),
  );
  const metrics = metricRows.flatMap((row) => {
    const definition = metricDefinitionBySeries.get(row.series_key);
    const value = Number(row.metric_value);
    const year = Number(row.latest_year);
    if (!definition || !Number.isFinite(value) || !Number.isInteger(year)) return [];
    return [{
      key: definition.key,
      label: definition.label,
      value,
      year,
      status: row.status,
      unit: definition.unit,
    }];
  });

  return {
    coverage: datasetCount === 0 ? "none" : datasetCount >= 10 ? "full" : "partial",
    datasetCount,
    metrics,
    sizeRows: sizeRows
      .map((row) => ({
        sizeCode: row.size_code,
        label: SIZE_LABELS[row.size_code] ?? row.size_code,
        enterprises: nullableNumber(row.enterprises),
        employment: nullableNumber(row.employment),
        turnoverEur: nullableNumber(row.turnover_eur),
        valueAddedEur: nullableNumber(row.value_added_eur),
        enterprisesYear: nullableNumber(row.enterprises_year),
        employmentYear: nullableNumber(row.employment_year),
        turnoverYear: nullableNumber(row.turnover_year),
        valueAddedYear: nullableNumber(row.value_added_year),
      }))
      .sort(
        (a, b) =>
          Object.keys(SIZE_LABELS).indexOf(a.sizeCode) -
          Object.keys(SIZE_LABELS).indexOf(b.sizeCode),
      ),
    latestYear: nullableNumber(coverageRow?.latest_year),
  };
}

function nullableNumber(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function emptyEurostatStats(): CountryEurostatBusinessStats {
  return {
    coverage: "none",
    datasetCount: 0,
    metrics: [],
    sizeRows: [],
    latestYear: null,
  };
}
