import { useEffect, useState } from "react";
import { Link, useParams } from "react-router";
import {
  ChevronLeft, ExternalLink, MapPin, Phone, Mail,
  Briefcase, Globe, Building2, Users, DollarSign,
  Calendar, Hash, ShieldCheck, FileText, Tag, Sparkles,
  Pencil, Check, X,
} from "lucide-react";
import { toast } from "sonner";
import { pgrest } from "~/lib/pgrest";
import { api } from "~/lib/api";
import type {
  VCompany, VCompanySource, VCompanyLocation, VCompanyPhone,
  VCompanyEmail, VCompanyIndustry, VCompanyMarket, VCompanyService,
  EnrichmentSourcesResponse, CompanyFinancial,
} from "~/types/api";
import { signalColor, confidenceColor, formatDate } from "~/lib/utils";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import { Skeleton } from "~/components/ui/skeleton";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Separator } from "~/components/ui/separator";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "~/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "~/components/ui/tabs";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter,
} from "~/components/ui/dialog";
import {
  Popover, PopoverContent, PopoverTrigger,
} from "~/components/ui/popover";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Textarea } from "~/components/ui/textarea";

// ── local types ───────────────────────────────────────────────────────────────

type VCompanyDomain = {
  id: string;
  company_id: string;
  domain_id: string;
  domain: string;
  relationship_type: string;
  status: string;
  signal: string;
  confidence: number;
  evidence: Record<string, unknown> | null;
  first_seen_at: string;
  last_seen_at: string;
};

// ── helpers ───────────────────────────────────────────────────────────────────

const STATUS_COLORS: Record<string, string> = {
  active:    "text-green-700 border-green-300 bg-green-50",
  inactive:  "text-yellow-700 border-yellow-300 bg-yellow-50",
  dissolved: "text-red-700 border-red-300 bg-red-50",
};

const LOCATION_TYPE_LABELS: Record<string, string> = {
  headquarters:       "Headquarters",
  registered_address: "Registered Address",
  office:             "Office",
};

const SOURCE_TYPE_LABELS: Record<string, string> = {
  company_registry:  "Company Registry",
  financial:         "Financial Database",
  open_data:         "Open Data",
  web:               "Web Crawl",
  global_aggregator: "Global Aggregator",
};

const FINANCIAL_STATUS_COLORS: Record<string, string> = {
  approved: "text-green-700 border-green-300 bg-green-50",
  rejected: "text-red-700 border-red-300 bg-red-50",
  suggested: "text-yellow-700 border-yellow-300 bg-yellow-50",
};

function formatMoney(cents: number | null): string | null {
  if (cents == null) return null;
  const dollars = cents / 100;
  if (Math.abs(dollars) >= 1_000_000) {
    return `$${(dollars / 1_000_000).toFixed(1)}M`;
  }
  return `$${Math.round(dollars / 1000)}k`;
}

// ── section wrapper ───────────────────────────────────────────────────────────

function Section({
  icon, title, children, count,
}: {
  icon: React.ReactNode;
  title: string;
  children: React.ReactNode;
  count?: number;
}) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm font-semibold flex items-center gap-2">
          {icon}
          {title}
          {count != null && (
            <span className="ml-auto text-xs font-normal text-muted-foreground">{count}</span>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  );
}

function EmptyState({ message }: { message: string }) {
  return <p className="text-sm text-muted-foreground">{message}</p>;
}

// ── identity facts row ────────────────────────────────────────────────────────

function Fact({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <p className="text-xs text-muted-foreground uppercase tracking-wide mb-0.5">{label}</p>
      <div className="text-sm font-medium truncate">{children}</div>
    </div>
  );
}

// ── edit dialog ───────────────────────────────────────────────────────────────

interface EditDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  company: VCompany;
  onSaved: (updated: VCompany) => void;
}

function EditDialog({ open, onOpenChange, company, onSaved }: EditDialogProps) {
  const [form, setForm] = useState({
    name: company.name ?? "",
    short_name: company.short_name ?? "",
    short_description: company.short_description ?? "",
    description: company.description ?? "",
    website: company.website ?? "",
    founded_year: company.founded_year != null ? String(company.founded_year) : "",
  });
  const [saving, setSaving] = useState(false);

  // Reset form when dialog opens with fresh company data
  useEffect(() => {
    if (open) {
      setForm({
        name: company.name ?? "",
        short_name: company.short_name ?? "",
        short_description: company.short_description ?? "",
        description: company.description ?? "",
        website: company.website ?? "",
        founded_year: company.founded_year != null ? String(company.founded_year) : "",
      });
    }
  }, [open, company]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!form.name.trim()) return;
    setSaving(true);
    try {
      const body: Parameters<typeof api.patchCompany>[1] = {
        name: form.name.trim() || undefined,
        short_name: form.short_name.trim() || undefined,
        short_description: form.short_description.trim() || undefined,
        description: form.description.trim() || undefined,
        website: form.website.trim() || undefined,
        founded_year: form.founded_year ? parseInt(form.founded_year, 10) : undefined,
      };
      const updated = await api.patchCompany(company.id, body);
      onSaved(updated);
      onOpenChange(false);
      toast.success("Company updated.");
    } catch {
      toast.error("Failed to update company.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Edit Company</DialogTitle>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="edit-name">Name <span className="text-destructive">*</span></Label>
            <Input
              id="edit-name"
              value={form.name}
              onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
              required
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="edit-short-name">Short Name</Label>
            <Input
              id="edit-short-name"
              value={form.short_name}
              onChange={(e) => setForm((f) => ({ ...f, short_name: e.target.value }))}
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="edit-short-desc">Short Description</Label>
            <Input
              id="edit-short-desc"
              value={form.short_description}
              onChange={(e) => setForm((f) => ({ ...f, short_description: e.target.value }))}
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="edit-desc">Description</Label>
            <Textarea
              id="edit-desc"
              rows={4}
              value={form.description}
              onChange={(e) => setForm((f) => ({ ...f, description: e.target.value }))}
            />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <Label htmlFor="edit-website">Website</Label>
              <Input
                id="edit-website"
                value={form.website}
                onChange={(e) => setForm((f) => ({ ...f, website: e.target.value }))}
                placeholder="https://..."
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="edit-founded">Founded Year</Label>
              <Input
                id="edit-founded"
                type="number"
                value={form.founded_year}
                onChange={(e) => setForm((f) => ({ ...f, founded_year: e.target.value }))}
                min={1800}
                max={new Date().getFullYear()}
              />
            </div>
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)} disabled={saving}>
              Cancel
            </Button>
            <Button type="submit" disabled={saving || !form.name.trim()}>
              {saving ? "Saving…" : "Save"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

// ── financial tab ─────────────────────────────────────────────────────────────

function FinancialTab({ companyId }: { companyId: string }) {
  const [financials, setFinancials] = useState<CompanyFinancial[]>([]);
  const [loading, setLoading] = useState(true);
  const [reviewingId, setReviewingId] = useState<string | null>(null);

  useEffect(() => {
    api.getCompanyFinancials(companyId)
      .then((res) => setFinancials(res.items))
      .catch(() => toast.error("Failed to load financial records."))
      .finally(() => setLoading(false));
  }, [companyId]);

  async function handleReview(id: string, action: "approve" | "reject") {
    setReviewingId(id);
    try {
      await api.reviewFinancial(id, action);
      toast.success(action === "approve" ? "Record approved." : "Record rejected.");
      const updated = await api.getCompanyFinancials(companyId);
      setFinancials(updated.items);
    } catch {
      toast.error("Failed to update record.");
    } finally {
      setReviewingId(null);
    }
  }

  if (loading) {
    return (
      <div className="space-y-2">
        <Skeleton className="h-8 w-full" />
        <Skeleton className="h-8 w-full" />
        <Skeleton className="h-8 w-full" />
      </div>
    );
  }

  if (financials.length === 0) {
    return (
      <Card>
        <CardContent className="pt-6">
          <EmptyState message="No financial records yet. Use Enrich to fetch from a source." />
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardContent className="p-0">
        <div className="rounded-md overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Year</TableHead>
                <TableHead>Source</TableHead>
                <TableHead>Employees</TableHead>
                <TableHead>Revenue (USD)</TableHead>
                <TableHead>Profit (USD)</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="w-20">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {financials.map((f) => {
                const rev = formatMoney(f.revenue_usd);
                const pft = formatMoney(f.profit_usd);
                const isNegProfit = f.profit_usd != null && f.profit_usd < 0;
                return (
                  <TableRow key={f.id}>
                    <TableCell className="font-mono">{f.year}</TableCell>
                    <TableCell className="text-sm text-muted-foreground">{f.source_name}</TableCell>
                    <TableCell className="text-sm">
                      {f.employee_count != null ? f.employee_count.toLocaleString() : "—"}
                    </TableCell>
                    <TableCell className="text-sm">{rev ?? "—"}</TableCell>
                    <TableCell className={`text-sm ${isNegProfit ? "text-red-600" : ""}`}>
                      {pft ?? "—"}
                    </TableCell>
                    <TableCell>
                      <Badge
                        variant="outline"
                        className={`text-xs ${FINANCIAL_STATUS_COLORS[f.status] ?? ""}`}
                      >
                        {f.status}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      {f.status === "suggested" && (
                        <div className="flex items-center gap-1">
                          <Button
                            size="icon"
                            variant="ghost"
                            className="h-6 w-6 text-green-700 hover:text-green-800 hover:bg-green-50"
                            disabled={reviewingId === f.id}
                            title="Approve"
                            onClick={() => handleReview(f.id, "approve")}
                          >
                            <Check className="size-3.5" />
                          </Button>
                          <Button
                            size="icon"
                            variant="ghost"
                            className="h-6 w-6 text-red-600 hover:text-red-700 hover:bg-red-50"
                            disabled={reviewingId === f.id}
                            title="Reject"
                            onClick={() => handleReview(f.id, "reject")}
                          >
                            <X className="size-3.5" />
                          </Button>
                        </div>
                      )}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}

// ── main page ─────────────────────────────────────────────────────────────────

export default function CompanyDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [company,    setCompany]    = useState<VCompany>();
  const [sources,    setSources]    = useState<VCompanySource[]>([]);
  const [domains,    setDomains]    = useState<VCompanyDomain[]>([]);
  const [locations,  setLocations]  = useState<VCompanyLocation[]>([]);
  const [phones,     setPhones]     = useState<VCompanyPhone[]>([]);
  const [emails,     setEmails]     = useState<VCompanyEmail[]>([]);
  const [industries, setIndustries] = useState<VCompanyIndustry[]>([]);
  const [markets,    setMarkets]    = useState<VCompanyMarket[]>([]);
  const [services,   setServices]   = useState<VCompanyService[]>([]);
  const [loading,    setLoading]    = useState(true);
  const [error,      setError]      = useState<string>();
  const [enrichSources, setEnrichSources] = useState<EnrichmentSourcesResponse | null>(null);
  const [enrichLoading, setEnrichLoading] = useState(false);
  const [editOpen, setEditOpen] = useState(false);

  useEffect(() => {
    if (!id) return;
    const q = `eq.${id}`;
    Promise.all([
      pgrest<VCompany>("v_companies",           { id: q, limit: 1 }),
      pgrest<VCompanySource>("v_company_sources",      { company_id: q }),
      pgrest<VCompanyDomain>("v_company_domains",      { company_id: q }),
      pgrest<VCompanyLocation>("v_company_locations",  { company_id: q }),
      pgrest<VCompanyPhone>("v_company_phones",        { company_id: q }),
      pgrest<VCompanyEmail>("v_company_emails",        { company_id: q }),
      pgrest<VCompanyIndustry>("v_company_industries", { company_id: q }),
      pgrest<VCompanyMarket>("v_company_markets",      { company_id: q }),
      pgrest<VCompanyService>("v_company_services",    { company_id: q }),
    ])
      .then(([c, src, dom, loc, ph, em, ind, mkt, svc]) => {
        if (c.data.length === 0) { setError("Company not found."); return; }
        setCompany(c.data[0]);
        setSources(src.data);
        setDomains(dom.data);
        setLocations(loc.data);
        setPhones(ph.data);
        setEmails(em.data);
        setIndustries(ind.data);
        setMarkets(mkt.data);
        setServices(svc.data);
        // Load enrichment sources in parallel (best-effort)
        api.getCompanyEnrichmentSources(id)
          .then(setEnrichSources)
          .catch(() => {});
      })
      .catch(() => setError("Failed to load company."))
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-40 w-full" />
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <Skeleton className="h-32 w-full" />
          <Skeleton className="h-32 w-full" />
        </div>
        <Skeleton className="h-48 w-full" />
      </div>
    );
  }
  if (error || !company) {
    return <Alert variant="destructive"><AlertDescription>{error}</AlertDescription></Alert>;
  }

  const empEst  = company.employee_estimate as { count?: number; min?: number; max?: number; label?: string };
  const revEst  = company.revenue_estimate  as { min?: number; max?: number; label?: string; currency?: string };
  const own     = company.ownership         as { type?: string; listed?: boolean; exchange?: string; ticker?: string };

  const hasContact   = phones.length > 0 || emails.length > 0;
  const hasBusiness  = industries.length > 0 || markets.length > 0 || services.length > 0;
  const hasEstimates = !!(empEst.label || empEst.count || revEst.label || company.employee_count != null || company.revenue_usd != null);
  const hasOwnership = !!(own.type || own.listed != null || own.exchange);

  return (
    <div className="space-y-4">
      {/* breadcrumb */}
      <div className="flex items-center gap-2">
        <Link to="/companies" className="text-sm text-muted-foreground hover:underline flex items-center gap-1">
          <ChevronLeft className="size-4" />Companies
        </Link>
      </div>

      {/* ── Identity card (always visible, outside tabs) ──────────────────── */}
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-start justify-between gap-4">
            <div className="min-w-0">
              <h1 className="text-xl font-bold leading-tight">{company.name}</h1>
              {company.short_name && company.short_name !== company.name && (
                <p className="text-sm text-muted-foreground mt-0.5">{company.short_name}</p>
              )}
            </div>
            <div className="flex items-center gap-2 shrink-0">
              <Badge variant="outline" className={STATUS_COLORS[company.status] ?? ""}>
                {company.status}
              </Badge>
              <Button size="sm" variant="outline" className="h-7 gap-1" onClick={() => setEditOpen(true)}>
                <Pencil className="size-3.5" />Edit
              </Button>
              {enrichSources && enrichSources.sources.length > 0 && (
                <Popover>
                  <PopoverTrigger asChild>
                    <Button size="sm" variant="outline" className="h-7 gap-1" disabled={enrichLoading}>
                      <Sparkles className="size-3.5" />Enrich
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent align="end" className="w-72 p-2 space-y-1">
                    <p className="text-xs text-muted-foreground px-2 py-1">
                      Missing: {enrichSources.missing_fields.join(", ")}
                    </p>
                    {enrichSources.sources.map((src) => (
                      <button
                        key={src.name}
                        className="w-full text-left rounded-md px-3 py-2 hover:bg-muted text-sm flex flex-col gap-0.5 disabled:opacity-50"
                        disabled={enrichLoading}
                        onClick={async () => {
                          setEnrichLoading(true);
                          try {
                            await api.enrichCompanyFromSource(id!, src.name);
                            toast.success(`Enrichment job queued from ${src.display_name ?? src.name}`);
                            setEnrichSources(null);
                          } catch {
                            toast.error("Failed to queue enrichment job.");
                          } finally {
                            setEnrichLoading(false);
                          }
                        }}
                      >
                        <span className="font-medium">{src.display_name ?? src.name}</span>
                        <span className="text-xs text-muted-foreground">Can provide: {src.can_provide.join(", ")}</span>
                      </button>
                    ))}
                  </PopoverContent>
                </Popover>
              )}
            </div>
          </div>
          {company.short_description && (
            <p className="text-sm text-muted-foreground mt-2 leading-relaxed">{company.short_description}</p>
          )}
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-x-6 gap-y-4 sm:grid-cols-3 lg:grid-cols-4">
            <Fact label="Country">
              {company.country_name}
              <span className="ml-1.5 font-mono text-xs text-muted-foreground">{company.country_iso2}</span>
            </Fact>
            {company.registration_number && (
              <Fact label="Registration #">
                <span className="font-mono">{company.registration_number}</span>
              </Fact>
            )}
            {company.lei && (
              <Fact label="LEI">
                <span className="font-mono text-xs break-all">{company.lei}</span>
              </Fact>
            )}
            {company.website && (
              <Fact label="Website">
                <a
                  href={company.website.startsWith("http") ? company.website : `https://${company.website}`}
                  target="_blank" rel="noopener noreferrer"
                  className="text-primary hover:underline flex items-center gap-1"
                >
                  {company.website.replace(/^https?:\/\//, "")}
                  <ExternalLink className="size-3 opacity-60 shrink-0" />
                </a>
              </Fact>
            )}
            {company.founded_year && (
              <Fact label="Founded">{company.founded_year}</Fact>
            )}
            {company.headquarters_location && (
              <Fact label="HQ Location">{company.headquarters_location}</Fact>
            )}
            <Fact label="Added">{formatDate(company.created_at)}</Fact>
            {company.primary_source_display_name && (
              <Fact label="Primary Source">
                <span className="text-muted-foreground">{company.primary_source_display_name}</span>
              </Fact>
            )}
          </div>
        </CardContent>
      </Card>

      {/* ── Edit dialog ───────────────────────────────────────────────────── */}
      <EditDialog
        open={editOpen}
        onOpenChange={setEditOpen}
        company={company}
        onSaved={setCompany}
      />

      {/* ── Tabs ──────────────────────────────────────────────────────────── */}
      <Tabs defaultValue="overview">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="financial">Financial</TabsTrigger>
          <TabsTrigger value="locations">
            Locations {locations.length > 0 && <span className="ml-1 text-xs opacity-70">{locations.length}</span>}
          </TabsTrigger>
          <TabsTrigger value="contacts">
            Contacts {hasContact && <span className="ml-1 text-xs opacity-70">{phones.length + emails.length}</span>}
          </TabsTrigger>
        </TabsList>

        {/* ── Overview tab ─────────────────────────────────────────────────── */}
        <TabsContent value="overview" className="space-y-4 mt-2">

          {/* Full description */}
          {company.description && (
            <Section icon={<FileText className="size-4" />} title="Description">
              <p className="text-sm leading-relaxed whitespace-pre-line">{company.description}</p>
            </Section>
          )}

          {/* Firmographics + Ownership */}
          {(hasEstimates || hasOwnership) && (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              {hasEstimates && (
                <Section icon={<Users className="size-4" />} title="Size & Revenue">
                  <dl className="space-y-2">
                    {(empEst.label || empEst.count) && (
                      <div className="flex items-center gap-2">
                        <Users className="size-3.5 text-muted-foreground shrink-0" />
                        <dt className="text-xs text-muted-foreground w-24 shrink-0">Employees</dt>
                        <dd className="text-sm font-medium">
                          {empEst.label ?? (empEst.count != null ? empEst.count.toLocaleString() : "—")}
                        </dd>
                      </div>
                    )}
                    {(revEst.label || company.revenue_usd != null) && (
                      <div className="flex items-center gap-2">
                        <DollarSign className="size-3.5 text-muted-foreground shrink-0" />
                        <dt className="text-xs text-muted-foreground w-24 shrink-0">Revenue</dt>
                        <dd className="text-sm font-medium">
                          {company.revenue_usd != null
                            ? `$${(company.revenue_usd / 100).toLocaleString(undefined, { maximumFractionDigits: 0 })} USD`
                            : revEst.label ?? "—"}
                        </dd>
                      </div>
                    )}
                  </dl>
                </Section>
              )}
              {hasOwnership && (
                <Section icon={<ShieldCheck className="size-4" />} title="Ownership">
                  <dl className="space-y-2">
                    {own.type && (
                      <div className="flex items-center gap-2">
                        <dt className="text-xs text-muted-foreground w-24 shrink-0">Type</dt>
                        <dd className="text-sm font-medium capitalize">{own.type}</dd>
                      </div>
                    )}
                    {own.listed != null && (
                      <div className="flex items-center gap-2">
                        <dt className="text-xs text-muted-foreground w-24 shrink-0">Listed</dt>
                        <dd className="text-sm font-medium">{own.listed ? "Yes" : "No"}</dd>
                      </div>
                    )}
                    {own.exchange && (
                      <div className="flex items-center gap-2">
                        <dt className="text-xs text-muted-foreground w-24 shrink-0">Exchange</dt>
                        <dd className="text-sm font-mono">{own.exchange}</dd>
                      </div>
                    )}
                    {own.ticker && (
                      <div className="flex items-center gap-2">
                        <dt className="text-xs text-muted-foreground w-24 shrink-0">Ticker</dt>
                        <dd className="text-sm font-mono">{own.ticker}</dd>
                      </div>
                    )}
                  </dl>
                </Section>
              )}
            </div>
          )}

          {/* Business profile */}
          {hasBusiness && (
            <Section icon={<Briefcase className="size-4" />} title="Business Profile">
              <div className="space-y-3">
                {industries.length > 0 && (
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide mb-1.5 flex items-center gap-1">
                      <Tag className="size-3" />Industries
                    </p>
                    <div className="flex flex-wrap gap-1.5">
                      {industries.map((i) => (
                        <Badge key={i.id} variant="secondary" className="text-xs">{i.industry}</Badge>
                      ))}
                    </div>
                  </div>
                )}
                {(industries.length > 0 && (markets.length > 0 || services.length > 0)) && <Separator />}
                {markets.length > 0 && (
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide mb-1.5">Markets</p>
                    <div className="flex flex-wrap gap-1.5">
                      {markets.map((m) => (
                        <Badge key={m.id} variant="outline" className="text-xs">{m.market}</Badge>
                      ))}
                    </div>
                  </div>
                )}
                {services.length > 0 && (
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide mb-1.5">Services / Products</p>
                    <div className="flex flex-wrap gap-1.5">
                      {services.map((s) => (
                        <Badge key={s.id} variant="outline" className="text-xs text-blue-700 border-blue-300 bg-blue-50">
                          {s.service}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </Section>
          )}

          {/* Associated Domains */}
          <Section icon={<Globe className="size-4" />} title="Associated Domains" count={domains.length}>
            {domains.length === 0 ? (
              <EmptyState message="No domains found for this company." />
            ) : (
              <div className="rounded-md border overflow-hidden">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Domain</TableHead>
                      <TableHead>Signal</TableHead>
                      <TableHead>Confidence</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>First Seen</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {domains.map((d) => (
                      <TableRow key={d.id}>
                        <TableCell>
                          <a
                            href={`https://${d.domain}`} target="_blank" rel="noopener noreferrer"
                            className="font-mono text-sm text-primary hover:underline inline-flex items-center gap-1"
                          >
                            {d.domain}
                            <ExternalLink className="size-3 opacity-60" />
                          </a>
                        </TableCell>
                        <TableCell>
                          <Badge className={signalColor(d.signal)} variant="outline">{d.signal}</Badge>
                        </TableCell>
                        <TableCell>
                          <span className={`font-bold ${confidenceColor(d.confidence)}`}>{d.confidence}</span>
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline" className="text-xs">{d.status}</Badge>
                        </TableCell>
                        <TableCell className="text-sm text-muted-foreground">{formatDate(d.first_seen_at)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </Section>

          {/* Discovery Sources */}
          {sources.length > 0 && (
            <Section icon={<Building2 className="size-4" />} title="Discovery Sources" count={sources.length}>
              <div className="rounded-md border overflow-hidden">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Source</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>External ID</TableHead>
                      <TableHead>Last Fetched</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {sources.map((s) => (
                      <TableRow key={s.source_id}>
                        <TableCell>
                          <Link
                            to={`/sources/${s.source_name}`}
                            className="font-medium hover:underline text-primary"
                          >
                            {s.source_display_name}
                          </Link>
                          <span className="ml-2 text-xs text-muted-foreground font-mono">{s.source_name}</span>
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline" className="text-xs">
                            {SOURCE_TYPE_LABELS[s.source_type] ?? s.source_type}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          {s.external_id
                            ? <span className="font-mono text-xs">{s.external_id}</span>
                            : <span className="text-muted-foreground">—</span>}
                        </TableCell>
                        <TableCell className="text-sm text-muted-foreground">
                          {s.fetched_at ? formatDate(s.fetched_at) : "—"}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            </Section>
          )}

          {/* Identifiers footer */}
          <Card className="bg-muted/30">
            <CardContent className="pt-4">
              <div className="flex flex-wrap gap-x-8 gap-y-2 text-xs text-muted-foreground">
                <span className="flex items-center gap-1.5"><Hash className="size-3" />ID: <span className="font-mono">{company.id}</span></span>
                {company.registration_number && (
                  <span className="flex items-center gap-1.5"><Hash className="size-3" />Reg: <span className="font-mono">{company.registration_number}</span></span>
                )}
                {company.lei && (
                  <span className="flex items-center gap-1.5"><Hash className="size-3" />LEI: <span className="font-mono">{company.lei}</span></span>
                )}
                <span className="flex items-center gap-1.5"><Calendar className="size-3" />Updated: {formatDate(company.updated_at)}</span>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* ── Financial tab ────────────────────────────────────────────────── */}
        <TabsContent value="financial" className="mt-2">
          <FinancialTab companyId={company.id} />
        </TabsContent>

        {/* ── Locations tab ────────────────────────────────────────────────── */}
        <TabsContent value="locations" className="mt-2">
          <Section icon={<MapPin className="size-4" />} title="Locations" count={locations.length}>
            {locations.length === 0 ? (
              <EmptyState message="No locations recorded." />
            ) : (
              <div className="rounded-md border overflow-hidden">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Type</TableHead>
                      <TableHead>Address</TableHead>
                      <TableHead>City</TableHead>
                      <TableHead>Region</TableHead>
                      <TableHead>Country</TableHead>
                      <TableHead className="text-xs text-muted-foreground">Source</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {locations.map((loc) => (
                      <TableRow key={loc.id}>
                        <TableCell>
                          <Badge variant="outline" className="text-xs whitespace-nowrap">
                            {LOCATION_TYPE_LABELS[loc.location_type] ?? loc.location_type}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-sm">
                          {[loc.address_line1, loc.address_line2].filter(Boolean).join(", ") || "—"}
                        </TableCell>
                        <TableCell className="text-sm">{loc.city ?? "—"}</TableCell>
                        <TableCell className="text-sm">{loc.region ?? "—"}</TableCell>
                        <TableCell className="text-sm">
                          {loc.country ?? "—"}
                          {loc.country_code && (
                            <span className="ml-1 text-muted-foreground font-mono text-xs">{loc.country_code}</span>
                          )}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">{loc.source}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </Section>
        </TabsContent>

        {/* ── Contacts tab ─────────────────────────────────────────────────── */}
        <TabsContent value="contacts" className="mt-2">
          {!hasContact ? (
            <Card>
              <CardContent className="pt-6">
                <EmptyState message="No contact information recorded." />
              </CardContent>
            </Card>
          ) : (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              {phones.length > 0 && (
                <Section icon={<Phone className="size-4" />} title="Phone Numbers" count={phones.length}>
                  <div className="divide-y rounded-md border overflow-hidden">
                    {phones.map((ph) => (
                      <div key={ph.id} className="px-4 py-2.5 flex items-center justify-between gap-2">
                        <div>
                          <a href={`tel:${ph.phone}`} className="text-sm font-mono hover:underline">
                            {ph.phone}
                          </a>
                          {ph.description && (
                            <p className="text-xs text-muted-foreground mt-0.5">{ph.description}</p>
                          )}
                        </div>
                        <Badge variant="outline" className="text-xs shrink-0">{ph.purpose}</Badge>
                      </div>
                    ))}
                  </div>
                </Section>
              )}
              {emails.length > 0 && (
                <Section icon={<Mail className="size-4" />} title="Email Addresses" count={emails.length}>
                  <div className="divide-y rounded-md border overflow-hidden">
                    {emails.map((em) => (
                      <div key={em.id} className="px-4 py-2.5 flex items-center justify-between gap-2">
                        <div>
                          <a href={`mailto:${em.email}`} className="text-sm hover:underline">{em.email}</a>
                          {em.name && (
                            <p className="text-xs text-muted-foreground mt-0.5">{em.name}</p>
                          )}
                        </div>
                        <Badge variant="outline" className="text-xs shrink-0">{em.purpose}</Badge>
                      </div>
                    ))}
                  </div>
                </Section>
              )}
            </div>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
