import { Form } from "react-router";
import type { Route } from "./+types/country-companies";
import { getCountry } from "~/lib/countries";
import { searchCompanies } from "~/lib/queries.server";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { DataTable } from "~/components/data-table/data-table";
import { DataTablePagination } from "~/components/data-table/pagination";
import { buildCompanyColumns } from "~/components/data-table/company-columns";

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });

  const url = new URL(request.url);
  const result = await searchCompanies(country, {
    q: url.searchParams.get("q") ?? "",
    page: Number(url.searchParams.get("page") ?? "1") || 1,
    pageSize: Number(url.searchParams.get("pageSize") ?? "50") || 50,
    sort: url.searchParams.get("sort"),
    dir: url.searchParams.get("dir"),
  });
  return { q: url.searchParams.get("q") ?? "", result, countryCode: country.code };
}

export default function CountryCompanies({ loaderData, params }: Route.ComponentProps) {
  const { q, result } = loaderData;
  const country = getCountry(params.country)!;
  const columns = buildCompanyColumns(country, result.sort, result.dir);

  return (
    <>
      <div className="flex flex-wrap items-center justify-between gap-4">
        <h2 className="text-xl font-semibold">Companies</h2>
        <Form method="get" className="flex gap-2">
          <Input
            type="search"
            name="q"
            defaultValue={q}
            placeholder="Search by name…"
            className="w-64"
          />
          <Button type="submit" variant="secondary">
            Search
          </Button>
        </Form>
      </div>

      <DataTable columns={columns} data={result.rows} />

      <DataTablePagination
        total={result.total}
        page={result.page}
        pageSize={result.pageSize}
      />
    </>
  );
}
