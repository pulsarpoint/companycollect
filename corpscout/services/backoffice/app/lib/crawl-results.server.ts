import { chQuery } from "~/lib/clickhouse.server";

export interface CrawlArchiveRow {
  domain: string;
  website_url: string;
  request_id: string;
  attempt: number | null;
  status: string;
  schema_version: string;
  source_path: string;
  result_json: string;
}

export async function readCrawlArchive(path: string | null): Promise<CrawlArchiveRow> {
  if (!path || path.length > 4096 || !/^[A-Za-z0-9._/-]+\/result\.json\.gz$/.test(path)
    || path.startsWith("/") || path.split("/").some(part => part === ".." || part === "." || !part)) {
    throw new Response("Select a saved crawl result.", {status: 400});
  }
  let rows: CrawlArchiveRow[];
  try {
    rows = await chQuery<CrawlArchiveRow>(`SELECT domain, website_url, request_id, attempt,
      status, schema_version, _path AS source_path, result_json
      FROM website_crawl_results_s3_archive
      WHERE _path = {path:String}
      LIMIT 2`, {path});
  } catch (error) {
    console.error("ClickHouse crawl archive read failed", {type: error instanceof Error ? error.name : "Unknown"});
    throw new Response("Cannot read the saved result through ClickHouse. Check the S3 mapping and try again.", {status: 503});
  }
  if (rows.length === 0) throw new Response("Saved result not found in the ClickHouse S3 mapping.", {status: 404});
  if (rows.length !== 1) throw new Response("The saved object contains more than one result.", {status: 502});
  return rows[0];
}
