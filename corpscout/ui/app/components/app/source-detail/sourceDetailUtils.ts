import type { DataSource } from "~/types/api";

const RAW_INPUT_TABLES = new Set([
  "brreg_workflow.raw_records",
  "cvr_workflow.raw_records",
  "france_workflow.raw_legal_units",
  "se_workflow.raw_records",
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

export function hasPipeline(source: DataSource): boolean {
  return source.download_workflow_registered;
}

export function hasCompanyExplorer(source: DataSource): boolean {
  return source.name === "finland_prhytj";
}

export function defaultSourceDetailPath(sourceName: string): string {
  if (sourceName === "finland_prhytj") return `/sources/${sourceName}/explorer`;
  if (sourceName === "brreg") return `/sources/${sourceName}/tasks`;
  if (sourceName === "ariregister" || sourceName === "cvr" || sourceName === "france" || sourceName === "se") return `/sources/${sourceName}/raw_input`;
  return `/sources/${sourceName}/config`;
}

export function sourceDetailTabs(source: DataSource): Array<{ label: string; to: string }> {
  const hasTaskTab = source.name === "brreg" || source.name === "france";
  const hasSourceEntriesTab =
    source.source_entries_available || source.name === "france";
  return [
    ...(hasTaskTab ? [{ label: "Tasks", to: `/sources/${source.name}/tasks` }] : []),
    ...(hasCompanyExplorer(source) ? [{ label: "Explorer", to: `/sources/${source.name}/explorer` }] : []),
    ...(hasSourceEntriesTab ? [{ label: "Source Entries", to: `/sources/${source.name}/source_entries` }] : []),
    ...(hasRawInputs(source) ? [{ label: "Raw Inputs", to: `/sources/${source.name}/raw_input` }] : []),
    { label: "Actions", to: `/sources/${source.name}/actions` },
    { label: "Schedule", to: `/sources/${source.name}/schedule` },
    { label: "Config", to: `/sources/${source.name}/config` },
    ...(hasPipeline(source) ? [{ label: "Pipeline", to: `/sources/${source.name}/pipeline` }] : []),
  ];
}
