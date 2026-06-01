import { useEffect, useState } from "react";
import { Link, useParams } from "react-router";
import { ArrowLeft, ExternalLink } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { getDomain } from "~/lib/api";
import { pgrest } from "~/lib/pgrest";
import type { DomainDetail } from "~/types/api";

interface CompanyDomainRow {
  id: string;
  company_id: string;
  signal: string;
  confidence: number;
  status: string;
}

export default function DomainDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [domain, setDomain] = useState<DomainDetail | null>(null);
  const [companies, setCompanies] = useState<CompanyDomainRow[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!id) return;
    Promise.all([
      getDomain(id),
      pgrest<CompanyDomainRow>("v_company_domains", { domain_id: `eq.${id}` }),
    ])
      .then(([d, c]) => {
        setDomain(d);
        setCompanies(c.data);
      })
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) {
    return <div className="p-8 text-muted-foreground">Loading…</div>;
  }

  if (!domain) {
    return <div className="p-8 text-destructive">Domain not found.</div>;
  }

  return (
    <div className="p-6 space-y-6 max-w-5xl mx-auto">
      <Link to="/domains" className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
        <ArrowLeft className="h-4 w-4" /> Domains
      </Link>

      <Card>
        <CardContent className="pt-6">
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-center gap-4">
              <div>
                <h1 className="text-2xl font-bold flex items-center gap-2">
                  {domain.domain}
                  <a
                    href={`https://${domain.domain}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-muted-foreground hover:text-foreground"
                  >
                    <ExternalLink className="h-4 w-4" />
                  </a>
                </h1>
                <div className="text-sm text-muted-foreground mt-1">
                  First seen: {new Date(domain.first_seen_at).toLocaleDateString()}
                  {" · "}
                  Companies: {companies.length}
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader><CardTitle>Linked Companies</CardTitle></CardHeader>
        <CardContent>
          {companies.length === 0 ? (
            <div className="text-sm text-muted-foreground">No linked companies.</div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Company</TableHead>
                  <TableHead>Signal</TableHead>
                  <TableHead>Confidence</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {companies.map((c) => (
                  <TableRow key={c.id}>
                    <TableCell>
                      <Link to={`/companies/${c.company_id}`} className="hover:underline text-sm font-mono">
                        {c.company_id.slice(0, 8)}…
                      </Link>
                    </TableCell>
                    <TableCell>{c.signal}</TableCell>
                    <TableCell>{c.confidence}</TableCell>
                    <TableCell>{c.status}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
