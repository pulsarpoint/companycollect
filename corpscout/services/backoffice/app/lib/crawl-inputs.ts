import type { DomainCrawlType } from "~/lib/se-domain-selection";

export interface CrawlInputStats {
  type: DomainCrawlType;
  total: number;
  enabled: number;
}

export interface CrawlInputRow {
  domain: string;
  website_url: string;
  enabled: boolean;
  priority: number;
  page_mode: "discover" | "explicit";
  pages: string[];
  headless: boolean;
  proxy_route: string;
  updated_at: string;
}

export interface CrawlInputsSnapshot {
  stats: CrawlInputStats[];
  type: DomainCrawlType;
  domain: string;
  rows: CrawlInputRow[];
  total: number;
  offset: number;
  limit: number;
}
