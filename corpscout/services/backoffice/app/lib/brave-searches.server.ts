import { randomUUID } from "node:crypto";
import { llmControl } from "~/lib/llm-control.server";
import { QUEUE_UUID } from "~/lib/queues";
import type { BraveSearch } from "~/lib/brave-searches";

export class BraveSearchError extends Error {}

const COLUMNS = `search_id AS "searchId", name, query_type AS "queryType", query_template AS "queryTemplate", revision`;

export function validateBraveSearch(name: string, template: string) {
  name = name.trim();
  template = template.replace(/\r\n?/g, "\n").trim();
  if (!name || name.length > 120 || /[\x00-\x1f\x7f]/.test(name)) throw new BraveSearchError("Enter a search name of 1–120 characters.");
  if (!template || template.length > 8000 || /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(template)) throw new BraveSearchError("Enter a question template of 1–8000 characters.");
  let companyField = false;
  const remainder = template.replace(/{{|}}|{(company_name|company_id|country_code)}/g, (_, field: string) => {
    if (field === "company_name" || field === "company_id") companyField = true;
    return "";
  });
  if (/[{}]/.test(remainder)) throw new BraveSearchError("Use only {company_name}, {company_id}, and {country_code}. Write literal braces as {{ and }}.");
  if (!companyField) throw new BraveSearchError("Include {company_name} or {company_id} so the question identifies each company.");
  return {name, queryTemplate: template};
}

export async function listBraveSearches(): Promise<BraveSearch[]> {
  return (await llmControl().query(`SELECT ${COLUMNS} FROM processing.brave_searches WHERE archived_at IS NULL ORDER BY lower(name),search_id`)).rows;
}

export async function saveBraveSearch(input: {searchId: string; revision: number; name: string; queryTemplate: string}) {
  const {name, queryTemplate} = validateBraveSearch(input.name, input.queryTemplate);
  if (input.searchId && (!QUEUE_UUID.test(input.searchId) || !Number.isSafeInteger(input.revision) || input.revision < 1)) throw new BraveSearchError("Invalid search version. Refresh and try again.");
  try {
    if (input.searchId) {
      const changed = await llmControl().query(`UPDATE processing.brave_searches SET name=$3,query_template=$4,
        revision=revision+1,updated_at=now() WHERE search_id=$1 AND revision=$2 AND archived_at IS NULL`,
        [input.searchId, input.revision, name, queryTemplate]);
      if (!changed.rowCount) throw new BraveSearchError("This search was changed or removed. Refresh before editing it again.");
    } else {
      const id = randomUUID();
      await llmControl().query(`INSERT INTO processing.brave_searches (search_id,name,query_type,query_template) VALUES ($1,$2,$3,$4)`,
        [id, name, `search_${id.replaceAll("-", "")}`, queryTemplate]);
    }
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "23505") throw new BraveSearchError("A search with this name already exists.");
    throw error;
  }
}

export async function removeBraveSearch(searchId: string, revision: number) {
  if (!QUEUE_UUID.test(searchId) || !Number.isSafeInteger(revision) || revision < 1) throw new BraveSearchError("Invalid search version.");
  const changed = await llmControl().query(`UPDATE processing.brave_searches SET archived_at=now(),updated_at=now()
    WHERE search_id=$1 AND revision=$2 AND archived_at IS NULL`, [searchId, revision]);
  if (!changed.rowCount) throw new BraveSearchError("This search was changed or removed. Refresh and try again.");
}

interface FrozenSearch {
  query_type: string; query_template: string;
  search_id?: string; search_name?: string; search_revision?: number;
}

async function frozenBraveSearch(taskId: string): Promise<FrozenSearch | null> {
  if (!QUEUE_UUID.test(taskId)) throw new BraveSearchError("Invalid Brave task ID.");
  const {rows} = await llmControl().query(`SELECT jsonb_strip_nulls(jsonb_build_object(
    'query_type',config#>'{execution,profile,query_type}', 'query_template',config#>'{execution,profile,query_template}',
    'search_id',config#>'{execution,profile,search_id}', 'search_name',config#>'{execution,profile,search_name}',
    'search_revision',config#>'{execution,profile,search_revision}')) AS search
    FROM processing.tasks WHERE task_id=$1 AND processor='brave-draft-v1' AND config ? 'execution'`, [taskId]);
  return rows[0]?.search ?? null;
}

export async function loadBraveSearchOptions(taskId: string) {
  const [searches, frozen] = await Promise.all([listBraveSearches(), frozenBraveSearch(taskId)]);
  return {searches, frozen};
}

/** Resolve current settings once; resumes use the execution's original snapshot. */
export async function resolveBraveSearch(taskId: string, searchId: string, revision: number): Promise<FrozenSearch> {
  const frozen = await frozenBraveSearch(taskId);
  if (frozen) {
    if (searchId !== "saved" || revision !== (frozen.search_revision ?? 0)) throw new BraveSearchError("This task has already started. Refresh to resume its saved search.");
    return frozen;
  }
  if (!QUEUE_UUID.test(searchId)) throw new BraveSearchError("Choose a saved Brave search before starting processing.");
  const {rows: [search]} = await llmControl().query<BraveSearch>(`SELECT ${COLUMNS} FROM processing.brave_searches
    WHERE search_id=$1 AND archived_at IS NULL`, [searchId]);
  if (!search) throw new BraveSearchError("This search was removed. Refresh and choose another search.");
  if (search.revision !== revision) throw new BraveSearchError("This search has changed. Refresh and review its new question before starting.");
  return {query_type: search.queryType, query_template: search.queryTemplate,
    search_id: search.searchId, search_name: search.name, search_revision: search.revision};
}
