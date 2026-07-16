import { Form, Link, useSearchParams } from "react-router";
import type { Route } from "./+types/country-companies";
import { getCountry } from "~/lib/countries";
import { searchCompanies } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

const PAGE_SIZE = 50;

export async function loader({ params, request }: Route.LoaderArgs) {
  const country = getCountry(params.country);
  if (!country) throw new Response("Not found", { status: 404 });

  const url = new URL(request.url);
  const q = url.searchParams.get("q") ?? "";
  const page = Number(url.searchParams.get("page") ?? "1") || 1;

  const result = await searchCompanies(country, { q, page, pageSize: PAGE_SIZE });
  return { q, result };
}

const nf = new Intl.NumberFormat("en-US");

export default function CountryCompanies({ loaderData }: Route.ComponentProps) {
  const { q, result } = loaderData;
  const [searchParams] = useSearchParams();
  const lastPage = Math.max(1, Math.ceil(result.total / result.pageSize));

  function pageLink(page: number) {
    const next = new URLSearchParams(searchParams);
    next.set("page", String(page));
    return `?${next.toString()}`;
  }

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

      <p className="text-muted-foreground text-sm">
        {nf.format(result.total)} companies
        {q ? ` matching “${q}”` : ""}
      </p>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-48">Registry ID</TableHead>
              <TableHead>Name</TableHead>
              <TableHead className="w-24">Status</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={3} className="text-muted-foreground h-24 text-center">
                  No companies found.
                </TableCell>
              </TableRow>
            ) : (
              result.rows.map((row) => (
                <TableRow key={`${row.id}-${row.name}`}>
                  <TableCell className="font-mono text-xs">{row.id}</TableCell>
                  <TableCell>{row.name}</TableCell>
                  <TableCell>
                    <Badge variant={row.active ? "default" : "outline"}>
                      {row.active ? "active" : "inactive"}
                    </Badge>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      <div className="flex items-center justify-between">
        <span className="text-muted-foreground text-sm">
          {`Page ${result.page} of ${nf.format(lastPage)}`}
        </span>
        <div className="flex gap-2">
          {result.page <= 1 ? (
            <Button variant="outline" size="sm" disabled>
              Previous
            </Button>
          ) : (
            <Button variant="outline" size="sm" render={<Link to={pageLink(result.page - 1)} />}>
              Previous
            </Button>
          )}
          {result.page >= lastPage ? (
            <Button variant="outline" size="sm" disabled>
              Next
            </Button>
          ) : (
            <Button variant="outline" size="sm" render={<Link to={pageLink(result.page + 1)} />}>
              Next
            </Button>
          )}
        </div>
      </div>
    </>
  );
}
