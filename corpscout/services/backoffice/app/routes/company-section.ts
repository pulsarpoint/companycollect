import type { Route } from "./+types/company-section";
import {
  getCompanySection,
  isCompanySectionName,
  type CompanySectionData,
} from "~/lib/company-sections.server";

export type CompanySectionResource =
  | { ok: true; data: CompanySectionData }
  | { ok: false; error: string };

export async function loader({ params }: Route.LoaderArgs): Promise<CompanySectionResource> {
  if (!isCompanySectionName(params.section)) {
    return { ok: false, error: "Unknown company section" };
  }
  try {
    return {
      ok: true,
      data: await getCompanySection(params.country, params.id, params.section),
    };
  } catch (error) {
    console.error("Company section failed", {
      country: params.country,
      companyId: params.id,
      section: params.section,
      error,
    });
    return { ok: false, error: "This section could not be loaded." };
  }
}
