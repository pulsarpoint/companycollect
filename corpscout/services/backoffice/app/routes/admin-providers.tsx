import { Link, useSearchParams } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import { Input } from "~/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
import type { ProviderSummary, TypeCounts, UnmappedKey } from "~/lib/domain-services";
import { getProviderSummaries, getUnmappedKeys } from "~/lib/domain-services.server";

type LoaderData = { error?: string; providers: ProviderSummary[]; unmapped: UnmappedKey[] };

export async function loader(): Promise<LoaderData> {
  try {
    const [providers, unmapped] = await Promise.all([getProviderSummaries(), getUnmappedKeys()]);
    return { providers, unmapped };
  } catch (error) {
    return { error: error instanceof Error ? error.message : String(error), providers: [], unmapped: [] };
  }
}

const COLUMNS: { label: string; types: string[] }[] = [
  { label: "DNS", types: ["dns"] },
  { label: "Mail", types: ["email"] },
  { label: "Sending", types: ["email_sending"] },
  { label: "Filtering", types: ["email_security"] },
  { label: "Web", types: ["hosting", "cdn"] },
];

function nowFor(byType: TypeCounts, types: string[]) {
  return types.reduce((sum, t) => sum + (byType[t]?.now ?? 0), 0);
}

const fmt = (n: number) => n.toLocaleString("en-US");

export default function ProvidersPage({ loaderData }: { loaderData: LoaderData }) {
  const [search] = useSearchParams();
  const q = (search.get("q") ?? "").trim().toLowerCase();
  const providers = loaderData.providers
    .filter((p) => !q || p.name.toLowerCase().includes(q) || p.slug.includes(q))
    .sort((a, b) => b.domainsNow - a.domainsNow || a.slug.localeCompare(b.slug));

  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="flex flex-col gap-1">
        <h1 className="text-2xl font-semibold">Providers</h1>
        <p className="text-muted-foreground text-sm">
          Domains using each provider, from their DNS records. Sums of per-type counts can exceed the total: one domain
          can use a provider for several services.
        </p>
      </header>
      {loaderData.error && (
        <Alert variant="destructive">
          <AlertTitle>Could not load provider counts</AlertTitle>
          <AlertDescription>{loaderData.error}</AlertDescription>
        </Alert>
      )}
      <form method="get" className="flex max-w-md gap-2">
        <Input name="q" defaultValue={search.get("q") ?? ""} placeholder="Search providers" aria-label="Search providers" />
        <Button type="submit" variant="outline">
          Search
        </Button>
      </form>
      <Card>
        <CardHeader>
          <CardTitle>Providers</CardTitle>
          <CardDescription>{fmt(providers.length)} providers</CardDescription>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Provider</TableHead>
                <TableHead>Category</TableHead>
                <TableHead className="text-right">Now</TableHead>
                <TableHead className="text-right">Ever</TableHead>
                {COLUMNS.map((c) => (
                  <TableHead key={c.label} className="text-right">
                    {c.label}
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {providers.map((p) => (
                <TableRow key={p.slug}>
                  <TableCell>
                    <Link
                      className="font-medium underline-offset-4 hover:underline"
                      to={`/admin/provider-feeds/providers/${encodeURIComponent(p.slug)}/domains`}
                    >
                      {p.name}
                    </Link>
                  </TableCell>
                  <TableCell>{p.category}</TableCell>
                  <TableCell className="text-right">{fmt(p.domainsNow)}</TableCell>
                  <TableCell className="text-right">{fmt(p.domainsEver)}</TableCell>
                  {COLUMNS.map((c) => (
                    <TableCell key={c.label} className="text-right">
                      {fmt(nowFor(p.byType, c.types))}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Unmapped provider domains</CardTitle>
          <CardDescription>
            Provider domains seen in DNS records that no provider definition names yet: candidates for provider-recon.
          </CardDescription>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Provider domain</TableHead>
                <TableHead className="text-right">Now</TableHead>
                <TableHead className="text-right">Ever</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loaderData.unmapped.map((u) => (
                <TableRow key={u.providerKey}>
                  <TableCell className="font-mono">{u.providerKey}</TableCell>
                  <TableCell className="text-right">{fmt(u.domainsNow)}</TableCell>
                  <TableCell className="text-right">{fmt(u.domainsEver)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
