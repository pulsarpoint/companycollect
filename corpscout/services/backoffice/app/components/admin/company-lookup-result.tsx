import { useEffect, useState } from "react";
import { Link } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

export interface CompanyLookupOutput {
  existing_company_ids?: string[];
  status: "already_mapped" | "matched" | "not_found" | "failed" | "cancelled";
  publication?: {state: "pending" | "published"; error?: string | null};
  site_info?: {site_description: string; purpose: string | null; operator_name: string | null; business_activities: string[]; evidence_status: string};
  site_info_result?: {crawl: {status: string; stop_reason: string}};
  found: boolean;
  company_id: string | null;
  confidence: number | null;
  site_type: string;
  reasons: string[];
  company: {legal_name: string} | null;
  searches: unknown[];
  candidates: unknown[];
  assessment?: {reasons: string[]};
  models?: {requested: string; served: string[]};
  candidate_assessments?: {company_id: string; legal_name: string; confidence: number; reasons: string[]}[];
  no_match_probability?: number;
  identity?: {kind: string; value: string; source_url: string; quote: string}[];
  industry_assessments?: {
    company_id: string; legal_name: string;
    status: "consistent" | "conflicting" | "insufficient_evidence";
    reasons: string[];
    industries: {classification_code: string; classification_version: string; reported_label: string; reference_label: string; reference_status: string; is_primary: number}[];
  }[];
}

export function CompanyLookupSummary({result}: {result: CompanyLookupOutput}) {
  return <section className="flex flex-col gap-3 rounded-md border p-4" aria-label="Company lookup result">
    <div className="flex flex-wrap items-center gap-2"><h3 className="font-semibold">{result.found ? "Company match proposed" : result.status === "already_mapped" ? "Company matching skipped · already connected" : result.status === "failed" ? "Company lookup failed" : result.status === "cancelled" ? "Company lookup cancelled" : "No company match found"}</h3><Badge variant="outline">{result.site_type.replaceAll("_", " ")}</Badge></div>
    {result.site_info && <section aria-label="Basic site information" className="flex flex-col gap-2">
      <div className="flex items-center gap-2"><h4 className="font-medium">Basic site information</h4><Badge variant="outline">{result.site_info_result?.crawl.status.replaceAll("_", " ") ?? result.site_info.evidence_status.replaceAll("_", " ")}</Badge></div>
      <p className="text-sm">{result.site_info.site_description}</p>
      {result.site_info.operator_name && <p className="text-sm">Website operator: {result.site_info.operator_name}</p>}
      {result.site_info.purpose && <p className="text-sm">Purpose: {result.site_info.purpose}</p>}
      {result.site_info.business_activities.length > 0 && <ul className="list-disc pl-5 text-sm">{result.site_info.business_activities.map((activity, index) => <li key={index}>{activity}</li>)}</ul>}
      {result.site_info_result?.crawl.status !== "finished" && <p className="text-sm text-muted-foreground">{result.site_info_result?.crawl.stop_reason.replaceAll("_", " ")}</p>}
    </section>}
    {result.existing_company_ids?.map(id => <p key={id}>Existing connection: <Link className="text-primary underline" to={`/admin/se/company/${encodeURIComponent(id)}`}>{id}</Link></p>)}
    {result.company_id && <p><Link className="text-primary underline" to={`/admin/se/company/${encodeURIComponent(result.company_id)}`}>{result.company?.legal_name} · {result.company_id}</Link></p>}
    {result.confidence !== null && <p className="text-sm">Confidence: {Math.round(result.confidence * 100)}% <span className="text-muted-foreground">(model estimate, not a calibrated probability)</span></p>}
    {result.assessment ? <>
      <p className="text-sm"><strong>Final evidence check:</strong> {result.reasons.at(-1)}</p>
      <details><summary className="cursor-pointer text-sm">Model assessment</summary><ul className="mt-2 list-disc pl-5 text-sm">{result.assessment.reasons.map((reason, i) => <li key={i}>{reason}</li>)}</ul></details>
    </> : <ul className="list-disc pl-5 text-sm">{result.reasons.map((reason, i) => <li key={i}>{reason}</li>)}</ul>}
    {Boolean(result.candidate_assessments?.length) && <section aria-label="Jev candidate ranking" className="flex flex-col gap-2">
      <h4 className="font-medium">Jev candidate ranking</h4>
      <p className="text-sm text-muted-foreground">Model scores describe its preference among these candidates; they are not calibrated match probabilities. No reliable match: {Math.round((result.no_match_probability ?? 0) * 100)}%.</p>
      <Table><TableHeader><TableRow><TableHead>Company</TableHead><TableHead>Model score</TableHead><TableHead>Evidence assessment</TableHead></TableRow></TableHeader><TableBody>
        {result.candidate_assessments?.map(candidate => <TableRow key={candidate.company_id}><TableCell><Link className="text-primary underline" to={`/admin/se/company/${encodeURIComponent(candidate.company_id)}`}>{candidate.legal_name} · {candidate.company_id}</Link></TableCell><TableCell>{Math.round(candidate.confidence * 100)}%</TableCell><TableCell className="whitespace-normal">{candidate.reasons.join(" ")}</TableCell></TableRow>)}
      </TableBody></Table>
    </section>}
    {Boolean(result.industry_assessments?.length) && <details>
      <summary className="cursor-pointer text-sm">NACE industry checks · {result.industry_assessments?.length} candidates</summary>
      <section aria-label="Industry consistency" className="mt-3 flex flex-col gap-3">
        <p className="text-sm text-muted-foreground">Industry supports identity; it cannot establish ownership. Missing or uncertain codes stay neutral, and organisation-number evidence takes priority.</p>
        <div><h4 className="font-medium">Website activities</h4>
          {result.identity?.some(fact => fact.kind === "business_activity") ? <ul className="list-disc pl-5 text-sm">{result.identity.filter(fact => fact.kind === "business_activity").map((fact, index) => <li key={index}>{fact.value} · <a className="text-primary underline" href={fact.source_url}>Source</a><p className="text-muted-foreground">“{fact.quote}”</p></li>)}</ul> : <p className="text-sm text-muted-foreground">No supported activity quotation was extracted.</p>}
        </div>
        <Table><TableHeader><TableRow><TableHead>Company</TableHead><TableHead>Registered industries</TableHead><TableHead>Consistency check</TableHead></TableRow></TableHeader><TableBody>
          {result.industry_assessments?.map(check => <TableRow key={check.company_id}>
            <TableCell className="whitespace-normal"><Link className="text-primary underline" to={`/admin/se/company/${encodeURIComponent(check.company_id)}`}>{check.legal_name} · {check.company_id}</Link></TableCell>
            <TableCell className="whitespace-normal">{check.industries.length ? <ul className="flex flex-col gap-2">{check.industries.map((industry, index) => <li key={index}>{industry.classification_version.replaceAll("_", " ") || "Unknown version"} · {industry.classification_code}{industry.is_primary ? " · Primary" : ""}<p>{industry.reported_label || industry.reference_label || "No description"}</p>{industry.reference_status !== "reference_consistent" && <p className="text-muted-foreground">Not used: {industry.reference_status.replaceAll("_", " ")}.</p>}</li>)}</ul> : "No registered industries"}</TableCell>
            <TableCell className="whitespace-normal"><Badge variant={check.status === "conflicting" ? "destructive" : "outline"}>{check.status.replaceAll("_", " ")}</Badge><p className="mt-2">{check.reasons.join(" ")}</p></TableCell>
          </TableRow>)}
        </TableBody></Table>
      </section>
    </details>}
    {result.models && <p className="text-sm text-muted-foreground">Requested model: {result.models.requested}. Reported model: {result.models.served.join(", ") || "Not reported"}.</p>}
    <p className="text-sm text-muted-foreground">{result.searches.length} database searches · {result.candidates.length} candidates · {result.publication ? result.publication.state === "published" ? "Findings saved to ClickHouse. Company–domain links await acceptance." : "Findings queued for ClickHouse. Company–domain links await acceptance." : "Historical test: no database results or associations saved."}</p>
    {result.publication?.error && <Alert><AlertTitle>Result delivery pending</AlertTitle><AlertDescription>{result.publication.error}. Saved findings remain in the crawler queue.</AlertDescription></Alert>}
    <details><summary className="cursor-pointer text-sm">Full lookup output</summary><pre className="mt-2 max-h-96 overflow-auto whitespace-pre-wrap break-words text-xs">{JSON.stringify(result, null, 2)}</pre></details>
  </section>;
}

export function CompanyLookupResult({url}: {url: string}) {
  const [result, setResult] = useState<CompanyLookupOutput | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    setResult(null); setError("");
    let timer: ReturnType<typeof setTimeout> | undefined;
    async function load() {
      try {
        const response = await fetch(url, {signal: controller.signal});
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Could not load company lookup output.");
        if (!controller.signal.aborted) {
          setResult(data);
          if (data.publication?.state === "pending") timer = setTimeout(load, 5000);
        }
      } catch (failure) {
        if (!controller.signal.aborted) setError(failure instanceof Error ? failure.message : "Could not load company lookup output.");
      }
    }
    void load();
    return () => {controller.abort(); clearTimeout(timer);};
  }, [url]);
  if (error) return <Alert variant="destructive"><AlertTitle>Company lookup output unavailable</AlertTitle><AlertDescription>{error}</AlertDescription></Alert>;
  return result ? <CompanyLookupSummary result={result} /> : <p className="text-sm">Loading company lookup output…</p>;
}
