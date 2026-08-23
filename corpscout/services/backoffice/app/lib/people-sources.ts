export const PEOPLE_SOURCE_NAMES = [
  "bolagsverket",
  "esef",
  "wikidata",
] as const;

export type PeopleSourceName = (typeof PEOPLE_SOURCE_NAMES)[number];

export interface PeopleSourceDefinition {
  name: PeopleSourceName;
  label: string;
  description: string;
  tables: readonly string[];
}

export const PEOPLE_SOURCE_CATALOG: readonly PeopleSourceDefinition[] = [
  {
    name: "bolagsverket",
    label: "Bolagsverket",
    description:
      "People and roles observed in Swedish annual-report signatures and certifications.",
    tables: ["se_financial_report_signatories"],
  },
  {
    name: "esef",
    label: "ESEF",
    description:
      "People extracted from visible ESEF report evidence by the document-processing model.",
    tables: ["esef_document_people"],
  },
  {
    name: "wikidata",
    label: "Wikidata",
    description:
      "Company-person role claims and their associated Wikidata person profiles.",
    tables: ["wikidata_company_people", "wikidata_persons"],
  },
] as const;

export function isPeopleSourceName(value: string): value is PeopleSourceName {
  return PEOPLE_SOURCE_NAMES.includes(value as PeopleSourceName);
}

export function getPeopleSourceDefinition(
  source: PeopleSourceName,
): PeopleSourceDefinition {
  return PEOPLE_SOURCE_CATALOG.find((item) => item.name === source)!;
}
