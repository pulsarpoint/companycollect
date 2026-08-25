/**
 * The two old list URLs moved under /admin/se/companies. These thin loader-only
 * routes keep bookmarks working: a GET must 302 to the new location.
 */
import { describe, expect, it } from "vitest";
import { loader as infoRedirect } from "~/routes/admin-se-company-info-redirect";
import { loader as geocodingRedirect } from "~/routes/admin-se-company-info-geocoding-redirect";

function locationOf(run: () => unknown): string {
  try {
    run();
  } catch (thrown) {
    expect(thrown).toBeInstanceOf(Response);
    const response = thrown as Response;
    expect(response.status).toBe(302);
    return response.headers.get("Location") ?? "";
  }
  throw new Error("loader did not redirect");
}

describe("old company-info URLs redirect under se/companies", () => {
  it("/admin/se/company-info -> /admin/se/companies (Info index)", () => {
    expect(locationOf(infoRedirect)).toBe("/admin/se/companies");
  });

  it("/admin/se/company-info/geocoding -> /admin/se/companies/geocoding", () => {
    expect(locationOf(geocodingRedirect)).toBe("/admin/se/companies/geocoding");
  });
});
