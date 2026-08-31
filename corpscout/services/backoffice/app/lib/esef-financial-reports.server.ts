import { chQuery } from "~/lib/clickhouse.server";
import type {
  EsefFinancialFact,
  EsefFinancialReport,
  EsefFinancialReportSummary,
} from "~/lib/esef-financial-reports";
import { parsePersistedEsefDisclosure } from "~/lib/esef-disclosures";

interface EsefReportSummaryRow {
  lei: string;
  fxo_id: string;
  entity_name: string;
  fiscal_year: number;
  period_end: string;
  currency: string;
  mapped_fact_count: number;
  source_fact_count: number;
  filing_version: number;
  viewer_url: string;
  source_url: string;
  package_url: string;
  error_count: number;
  warning_count: number;
  date_added: string;
}

interface EsefFactRow {
  fact_id: string;
  concept_qname: string;
  concept_local_name: string;
  value_kind: string;
  raw_value: string;
  amount_original: number | null;
  amount_usd: number | null;
  fx_rate_date: string;
  fx_source: string;
  decimals: number | null;
  period_start: string;
  period_instant: string;
  period_duration_end: string;
  unit: string;
  currency: string;
  dimensions: string;
  language: string;
  concept_labels_json: string;
  concept_documentation_json: string;
  disclosure_blocks_json: string;
  disclosure_plain_text: string;
  disclosure_source_record_uid: string;
  disclosure_text_sha256: string;
  disclosure_parser_name: string;
  disclosure_parser_version: string;
}

interface AvailableTableRow {
  name: string;
}

const REPORT_SUMMARY_QUERY = `
SELECT
  f.lei AS lei,
  f.fxo_id AS fxo_id,
  f.entity_name AS entity_name,
  coalesce(m.fiscal_year, toYear(f.period_end)) AS fiscal_year,
  toString(f.period_end) AS period_end,
  coalesce(m.currency, '') AS currency,
  coalesce(m.mapped_fact_count, 0) AS mapped_fact_count,
  coalesce(m.source_fact_count, 0) AS source_fact_count,
  toUInt32(extract(f.fxo_id, '-([0-9]+)$')) AS filing_version,
  f.viewer_url AS viewer_url,
  f.source_url AS source_url,
  f.package_url AS package_url,
  f.error_count AS error_count,
  f.warning_count AS warning_count,
  toString(f.date_added) AS date_added
FROM corpscout.esef_filings AS f FINAL
LEFT JOIN corpscout.esef_financial_metrics AS m FINAL
  ON m.lei = f.lei AND m.period_end = f.period_end AND m.fxo_id = f.fxo_id
INNER JOIN corpscout.company_identifier AS identifier
  ON identifier.issuer_scheme = 'lei'
 AND identifier.issuer_id = upperUTF8(trimBoth(f.lei))
WHERE identifier.country_code = {country:String}
  AND identifier.company_id = {id:String}
  AND f.fxo_id = {documentId:String}
ORDER BY f.resolved_at DESC
LIMIT 1`;

const OPTIONAL_TABLES_QUERY = `
SELECT name
FROM system.tables
WHERE database = 'corpscout'
  AND name IN ('esef_disclosures', 'esef_document_concept_labels')`;

function reportFactsQuery(
  withDisclosures: boolean,
  withConceptLabels: boolean,
): string {
  const disclosureColumns = withDisclosures
    ? `
  coalesce(disclosures.blocks_json, '') AS disclosure_blocks_json,
  coalesce(disclosures.plain_text, '') AS disclosure_plain_text,
  coalesce(toString(disclosures.source_record_uid), '') AS disclosure_source_record_uid,
  coalesce(toString(disclosures.text_sha256), '') AS disclosure_text_sha256,
  coalesce(disclosures.parser_name, '') AS disclosure_parser_name,
  coalesce(toString(disclosures.parser_version), '') AS disclosure_parser_version`
    : `
  '' AS disclosure_blocks_json,
  '' AS disclosure_plain_text,
  '' AS disclosure_source_record_uid,
  '' AS disclosure_text_sha256,
  '' AS disclosure_parser_name,
  '' AS disclosure_parser_version`;
  const disclosureJoin = withDisclosures
    ? `
LEFT ANY JOIN (
  SELECT
    source_document_id,
    source_fact_id,
    blocks_json,
    plain_text,
    source_record_uid,
    text_sha256,
    parser_name,
    parser_version
  FROM corpscout.esef_disclosures
  WHERE disclosure_kind = 'tagged_fact'
    AND source_document_id = {documentId:String}
) AS disclosures
  ON disclosures.source_document_id = facts.fxo_id
 AND disclosures.source_fact_id = facts.fact_id`
    : "";
  const conceptLabelColumn = withConceptLabels
    ? `coalesce(labels.concept_labels_json, '[]') AS concept_labels_json,
  coalesce(labels.concept_documentation_json, '[]') AS concept_documentation_json,`
    : `'[]' AS concept_labels_json,
  '[]' AS concept_documentation_json,`;
  const conceptLabelJoin = withConceptLabels
    ? `
LEFT ANY JOIN (
  SELECT
    source_document_id,
    concept_qname,
    toJSONString(arraySort(
      groupArrayIf(
        tuple(
          language,
          label,
          is_report_language,
          text_source,
          translation_provider,
          translation_model,
          translation_version
        ),
        label_role = 'standard'
      )
    )) AS concept_labels_json,
    toJSONString(arraySort(
      groupArrayIf(
        tuple(
          language,
          label,
          is_report_language,
          text_source,
          translation_provider,
          translation_model,
          translation_version
        ),
        label_role = 'documentation'
      )
    )) AS concept_documentation_json
  FROM (
    SELECT
      source_document_id,
      concept_qname,
      language,
      label,
      is_report_language,
      label_role,
      'taxonomy' AS text_source,
      '' AS translation_provider,
      '' AS translation_model,
      toUInt64(0) AS translation_version
    FROM corpscout.esef_document_concept_labels

    UNION ALL

    SELECT DISTINCT
      source.source_document_id,
      source.concept_qname,
      'en' AS language,
      translation.translated_text AS label,
      toUInt8(0) AS is_report_language,
      source.label_role,
      'translation' AS text_source,
      translation.provider AS translation_provider,
      translation.model AS translation_model,
      translation.version AS translation_version
    FROM corpscout.esef_document_concept_labels AS source
    INNER JOIN (
      SELECT
        source_text_hash,
        source_lang,
        tupleElement(latest, 1) AS translated_text,
        tupleElement(latest, 2) AS provider,
        tupleElement(latest, 3) AS model,
        max_version AS version
      FROM (
        SELECT
          source_text_hash,
          source_lang,
          argMax(tuple(translated_text, provider, model), version) AS latest,
          max(version) AS max_version
        FROM corpscout.text_translations
        WHERE source_table = 'corpscout.esef_document_concept_labels'
          AND source_column = 'label'
          AND target_lang = 'en'
        GROUP BY source_text_hash, source_lang
      )
    ) AS translation
      ON translation.source_lang = source.language
     AND translation.source_text_hash = cityHash64(source.label)
    LEFT ANTI JOIN (
      SELECT DISTINCT source_document_id, concept_qname, label_role
      FROM corpscout.esef_document_concept_labels
      WHERE label != ''
        AND (language = 'en' OR startsWith(language, 'en-'))
    ) AS official_english
      ON official_english.source_document_id = source.source_document_id
     AND official_english.concept_qname = source.concept_qname
     AND official_english.label_role = source.label_role
    WHERE source.is_report_language
      AND source.label != ''
      AND source.language != 'en'
      AND NOT startsWith(source.language, 'en-')
  ) AS concept_text
  GROUP BY source_document_id, concept_qname
) AS labels
  ON labels.source_document_id = facts.fxo_id
 AND labels.concept_qname = facts.concept_qname`
    : "";
  return `
SELECT
  facts.fact_id AS fact_id,
  facts.concept_qname AS concept_qname,
  facts.concept_local_name AS concept_local_name,
  facts.value_kind AS value_kind,
  facts.raw_value AS raw_value,
  toFloat64(facts.amount_original) AS amount_original,
  multiIf(
    facts.amount_original IS NULL, CAST(NULL AS Nullable(Float64)),
    facts.currency = 'USD', toFloat64(facts.amount_original),
    facts.currency = metrics.currency AND metrics.fx_rate_to_usd IS NOT NULL,
      toFloat64(facts.amount_original) * metrics.fx_rate_to_usd,
    CAST(NULL AS Nullable(Float64))
  ) AS amount_usd,
  if(
    facts.currency NOT IN ('', 'USD') AND facts.currency = metrics.currency,
    coalesce(toString(metrics.fx_rate_date), ''),
    ''
  ) AS fx_rate_date,
  if(
    facts.currency NOT IN ('', 'USD') AND facts.currency = metrics.currency,
    metrics.fx_source,
    ''
  ) AS fx_source,
  facts.decimals AS decimals,
  coalesce(toString(facts.period_start), '') AS period_start,
  coalesce(toString(facts.period_instant), '') AS period_instant,
  coalesce(toString(facts.period_duration_end), '') AS period_duration_end,
  facts.unit AS unit,
  facts.currency AS currency,
  facts.dimensions AS dimensions,
  facts.language AS language,
  ${conceptLabelColumn}
${disclosureColumns}
FROM corpscout.esef_facts AS facts FINAL
LEFT JOIN corpscout.esef_financial_metrics AS metrics FINAL
  ON metrics.lei = facts.lei
 AND metrics.period_end = facts.period_end
 AND metrics.fxo_id = facts.fxo_id
${disclosureJoin}
${conceptLabelJoin}
WHERE facts.lei = {lei:String}
  AND facts.period_end = {periodEnd:Date32}
  AND facts.fxo_id = {documentId:String}
ORDER BY facts.concept_qname, facts.period_instant DESC,
  facts.period_duration_end DESC, facts.fact_id`;
}

function parseConceptLabels(raw: string): EsefFinancialFact["conceptLabels"] {
  if (!raw) return [];
  try {
    const value: unknown = JSON.parse(raw);
    if (!Array.isArray(value)) return [];
    return value.flatMap((entry) => {
      if (
        !Array.isArray(entry) ||
        typeof entry[0] !== "string" ||
        typeof entry[1] !== "string"
      ) {
        return [];
      }
      return [
        {
          language: entry[0],
          label: entry[1],
          isReportLanguage: Boolean(entry[2]),
          source:
            entry[3] === "taxonomy" ||
            entry[3] === "translation" ||
            entry[3] === "identifier"
              ? entry[3]
              : undefined,
          translationProvider:
            typeof entry[4] === "string" && entry[4] !== ""
              ? entry[4]
              : undefined,
          translationModel:
            typeof entry[5] === "string" && entry[5] !== ""
              ? entry[5]
              : undefined,
          translationVersion:
            typeof entry[6] === "number" && entry[6] > 0
              ? entry[6]
              : undefined,
        },
      ];
    });
  } catch {
    return [];
  }
}

function reportSummary(
  row: EsefReportSummaryRow,
  factCount: number,
): EsefFinancialReportSummary {
  return {
    fxoId: row.fxo_id,
    entityName: row.entity_name,
    fiscalYear: Number(row.fiscal_year),
    periodEnd: row.period_end,
    currency: row.currency,
    factCount,
    mappedFactCount: Number(row.mapped_fact_count),
    sourceFactCount: Number(row.source_fact_count),
    filingVersion: Number(row.filing_version),
    viewerUrl: row.viewer_url,
    sourceUrl: row.source_url,
    packageUrl: row.package_url,
    errorCount: Number(row.error_count),
    warningCount: Number(row.warning_count),
    dateAdded: row.date_added,
  };
}

export async function getEsefFinancialReport(
  countryCode: string,
  companyId: string,
  documentId: string,
): Promise<EsefFinancialReport | null> {
  const params = {
    country: countryCode.toUpperCase(),
    id: companyId,
    documentId,
  };
  const summaries = await chQuery<EsefReportSummaryRow>(
    REPORT_SUMMARY_QUERY,
    params,
  );
  if (!summaries[0]) return null;
  const optionalTables = new Set(
    (await chQuery<AvailableTableRow>(OPTIONAL_TABLES_QUERY)).map(
      (row) => row.name,
    ),
  );
  const rows = await chQuery<EsefFactRow>(
    reportFactsQuery(
      optionalTables.has("esef_disclosures"),
      optionalTables.has("esef_document_concept_labels"),
    ),
    {
      ...params,
      lei: summaries[0].lei,
      periodEnd: summaries[0].period_end,
    },
  );
  const facts: EsefFinancialFact[] = rows.map((row) => ({
    factId: row.fact_id,
    conceptQname: row.concept_qname,
    conceptLocalName: row.concept_local_name,
    valueKind: row.value_kind,
    rawValue: row.raw_value,
    amountOriginal:
      row.amount_original === null ? null : Number(row.amount_original),
    amountUsd: row.amount_usd === null ? null : Number(row.amount_usd),
    fxRateDate: row.fx_rate_date,
    fxSource: row.fx_source,
    decimals: row.decimals === null ? null : Number(row.decimals),
    periodStart: row.period_start,
    periodInstant: row.period_instant,
    periodDurationEnd: row.period_duration_end,
    unit: row.unit,
    currency: row.currency,
    dimensions: row.dimensions,
    language: row.language,
    conceptLabels: parseConceptLabels(row.concept_labels_json),
    conceptDocumentation: parseConceptLabels(
      row.concept_documentation_json,
    ),
    structuredDisclosure: parsePersistedEsefDisclosure(
      row.disclosure_blocks_json,
      row.disclosure_plain_text,
    ),
    disclosureEvidence: row.disclosure_source_record_uid
      ? {
          sourceRecordUid: row.disclosure_source_record_uid,
          textSha256: row.disclosure_text_sha256,
          parserName: row.disclosure_parser_name,
          parserVersion: row.disclosure_parser_version,
        }
      : null,
  }));
  return { summary: reportSummary(summaries[0], facts.length), facts };
}
