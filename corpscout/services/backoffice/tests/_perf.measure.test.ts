import { describe, it } from "vitest";
import { searchUnifiedCompanies } from "~/lib/unified.server";

async function timed<T>(label: string, fn: () => Promise<T>): Promise<T> {
  const t0 = performance.now();
  const result = await fn();
  const t1 = performance.now();
  console.log(`[PERF] ${label}: ${(t1 - t0).toFixed(1)}ms`);
  return result;
}

describe("perf measurements", () => {
  it("runs the measurement suite", async () => {
    // default page, cold then warm
    await timed("default page (cold)", () => searchUnifiedCompanies({}));
    await timed("default page (warm)", () => searchUnifiedCompanies({}));

    // deep name-sort page 300
    await timed("name sort page 300 (cold)", () =>
      searchUnifiedCompanies({ sort: "name", dir: "asc", page: 300, pageSize: 50 }),
    );
    await timed("name sort page 300 (warm)", () =>
      searchUnifiedCompanies({ sort: "name", dir: "asc", page: 300, pageSize: 50 }),
    );

    // petrobras search
    await timed("petrobras search (cold)", () =>
      searchUnifiedCompanies({ q: "petrobras", pageSize: 25 }),
    );
    await timed("petrobras search (warm)", () =>
      searchUnifiedCompanies({ q: "petrobras", pageSize: 25 }),
    );

    // revenue sort
    await timed("revenue sort (cold)", () =>
      searchUnifiedCompanies({ sort: "revenue", dir: "desc", pageSize: 25 }),
    );
    await timed("revenue sort (warm)", () =>
      searchUnifiedCompanies({ sort: "revenue", dir: "desc", pageSize: 25 }),
    );
  }, 120_000);
});
