import { chQuery } from "~/lib/clickhouse.server";

export interface CompanyPersonRoleType {
  role_code: string;
  display_name: string;
  role_group: string;
  description: string;
  is_active: number;
  created_at: string;
  updated_at: string;
}

export const COMPANY_PERSON_ROLE_TYPES_QUERY = `SELECT
  role_code,
  display_name,
  toString(role_group) AS role_group,
  description,
  toUInt8(is_active) AS is_active,
  toString(created_at) AS created_at,
  toString(updated_at) AS updated_at
FROM corpscout.company_person_role_type FINAL
ORDER BY
  multiIf(
    role_group = 'governance', 1,
    role_group = 'executive', 2,
    role_group = 'audit', 3,
    role_group = 'ownership', 4,
    5
  ),
  display_name,
  role_code`;

export function getCompanyPersonRoleTypes(): Promise<CompanyPersonRoleType[]> {
  return chQuery<CompanyPersonRoleType>(COMPANY_PERSON_ROLE_TYPES_QUERY);
}
