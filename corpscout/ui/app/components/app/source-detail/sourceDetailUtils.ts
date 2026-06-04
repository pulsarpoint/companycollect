import type { DataSource } from "~/types/api";

const RAW_INPUT_TABLES = new Set([
  "brreg_workflow.raw_records",
  "cvr_workflow.raw_records",
  "gleif_company_raw_inputs",
  "companies_house_company_raw_inputs",
  "ariregister_company_raw_inputs",
  "ariregister_workflow.raw_records",
]);

export function hasRawInputs(source: DataSource): boolean {
  return RAW_INPUT_TABLES.has(source.input_table_name);
}

export function sourceDisplayName(source: DataSource): string {
  return source.display_name || source.name;
}

export function sourceInputDisplayName(source: DataSource): string {
  return source.input_table_name;
}

export function validateDuration(value: string): string | undefined {
  if (!/^\d+[hms]$/.test(value.trim())) {
    return "Use a Go duration such as 24h, 12h, or 30m.";
  }
  return undefined;
}

export function validateCronExpression(value: string): string | undefined {
  const expression = value.trim();
  if (!expression) {
    return "Use a cron expression such as 0 4 * * *.";
  }
  if (expression.startsWith("@")) {
    return undefined;
  }
  if (expression.split(/\s+/).length !== 5) {
    return "Use five cron fields: minute hour day-of-month month day-of-week.";
  }
  return undefined;
}

export function hasPipeline(source: DataSource): boolean {
  return source.download_workflow_registered;
}

export function defaultSourceDetailPath(sourceName: string): string {
  if (sourceName === "brreg") return `/sources/${sourceName}/tasks`;
  if (sourceName === "ariregister" || sourceName === "cvr") return `/sources/${sourceName}/raw_input`;
  return `/sources/${sourceName}/schedule`;
}

export function sourceDetailTabs(source: DataSource): Array<{ label: string; to: string }> {
  const hasTaskTab = source.name === "brreg";
  const hasSourceEntriesTab = source.source_entries_available;
  return [
    ...(hasTaskTab ? [{ label: "Tasks", to: `/sources/${source.name}/tasks` }] : []),
    ...(hasSourceEntriesTab ? [{ label: "Source Entries", to: `/sources/${source.name}/source_entries` }] : []),
    ...(source.name !== "brreg" ? [{ label: "Schedule", to: `/sources/${source.name}/schedule` }] : []),
    { label: "Config", to: `/sources/${source.name}/config` },
    ...(hasRawInputs(source) ? [{ label: "Raw Inputs", to: `/sources/${source.name}/raw_input` }] : []),
    ...(hasPipeline(source) ? [{ label: "Pipeline", to: `/sources/${source.name}/pipeline` }] : []),
  ];
}
