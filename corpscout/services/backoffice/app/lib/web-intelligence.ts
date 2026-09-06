export interface WebIntelligenceCrawlCoverage {
  crawlId: string;
  observedPages: number;
  observedAt: string;
}

export interface WebOrganizationClaim {
  crawlId: string;
  pageUrl: string;
  entityTypes: string[];
  name: string;
  legalName: string;
  description: string;
  entityUrl: string;
  logo: string;
  email: string;
  telephone: string;
  sameAs: string[];
  country: string;
  foundingYear: number | null;
  employeeCount: number | null;
  observedAt: string;
}

export interface WebContactObservation {
  type: string;
  value: string;
  source: string;
  sourceUrl: string;
  lastObservedCrawl: string;
  observedAt: string;
}

export interface WebAddressObservation {
  value: string;
  sourceUrl: string;
  firstObservedCrawl: string;
  lastObservedCrawl: string;
  observedCrawls: number;
  observedAt: string;
}

export interface WebIdentifierObservation {
  type: string;
  value: string;
  sources: string[];
  firstObservedCrawl: string;
  lastObservedCrawl: string;
  observedCrawls: number;
  observedPages: number;
  sampleUrls: string[];
}

export interface WebIndustryObservation {
  naceCode: string;
  naceLabel: string;
  rank: number;
  isPrimary: boolean;
  score: number;
  method: string;
  sourceUrl: string;
}

export interface WebIndustrySnapshot {
  crawlId: string;
  pageType: string;
  pageTypeScore: number | null;
  classificationConfident: boolean | null;
  classificationMargin: number | null;
  sourceUrl: string;
  observedAt: string;
  industries: WebIndustryObservation[];
}

export interface WebPageMetadataSnapshot {
  crawlId: string;
  sourceUrl: string;
  title: string;
  meta: Record<string, string>;
  canonical: string;
  hreflang: string[];
  jsonLdTypes: string[];
  charset: string;
  observedAt: string;
}

export interface WebSecuritySnapshot {
  crawlId: string;
  sourceUrl: string;
  headers: Record<string, string>;
  observedAt: string;
}

export interface WebAuthoritySnapshot {
  crawlId: string;
  harmonicCentrality: number;
  harmonicRank: number;
  pageRank: number;
  pageRankRank: number;
  observedHosts: number;
  observedAt: string;
}

export interface CompanyWebIntelligence {
  domain: string;
  crawlCoverage: WebIntelligenceCrawlCoverage[];
  organizationClaims: WebOrganizationClaim[];
  addresses: WebAddressObservation[];
  contacts: WebContactObservation[];
  identifiers: WebIdentifierObservation[];
  industrySnapshots: WebIndustrySnapshot[];
  pageMetadataSnapshots: WebPageMetadataSnapshot[];
  securitySnapshots: WebSecuritySnapshot[];
  authoritySnapshots: WebAuthoritySnapshot[];
  truncated: {
    organizationClaims: boolean;
    addresses: boolean;
    contacts: boolean;
    identifiers: boolean;
  };
}
