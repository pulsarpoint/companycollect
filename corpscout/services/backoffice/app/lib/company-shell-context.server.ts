import { createContext } from "react-router";
import type { CompanyShell } from "~/lib/queries.server";
import type { CompanySectionPresence } from "~/lib/company-sections.server";

export const companyShellPromiseContext =
  createContext<Promise<CompanyShell | null>>();

export const companySectionPresencePromiseContext =
  createContext<Promise<CompanySectionPresence[]>>();
