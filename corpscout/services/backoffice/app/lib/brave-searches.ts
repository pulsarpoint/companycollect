export interface BraveSearch {
  searchId: string;
  name: string;
  queryType: string;
  queryTemplate: string;
  revision: number;
}

export function braveSearchPreview(template: string): string {
  const example: Record<string, string> = {company_name: "Example AB", company_id: "5560123456", country_code: "SE"};
  return template.replace(/{{|}}|{(company_name|company_id|country_code)}/g,
    (token, field: string) => field ? example[field] : token === "{{" ? "{" : "}");
}
