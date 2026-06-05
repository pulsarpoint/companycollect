import { useEffect, useMemo, useState } from "react";
import { Link, useOutletContext, useParams } from "react-router";
import { ChevronLeft, ExternalLink } from "lucide-react";
import { api, errorMessage } from "~/lib/api";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import type {
  AriregisterSourceCompanyDetail,
  BrregSourceCompanyDetail,
} from "~/types/api";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import { Separator } from "~/components/ui/separator";
import { Skeleton } from "~/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

function textValue(value: unknown): string {
  if (value == null || value === "") return "-";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number") return value.toLocaleString();
  return String(value);
}

function dateValue(value: unknown): string {
  if (!value) return "-";
  if (typeof value !== "string") return textValue(value);
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString();
}

function rowsFrom(value: unknown): Array<Record<string, unknown>> {
  return Array.isArray(value) ? value : [];
}

function numberValue(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
}

function stringValue(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed === "" ? undefined : trimmed;
}

function formatUsdCents(value: unknown): string {
  const cents = numberValue(value);
  if (cents == null) return "-";
  return new Intl.NumberFormat(undefined, {
    currency: "USD",
    maximumFractionDigits: 0,
    style: "currency",
  }).format(cents / 100);
}

function formatOriginalAmount(amount: unknown, currency: unknown): string {
  const numericAmount = numberValue(amount);
  if (numericAmount == null) return "-";
  const currencyCode = stringValue(currency);
  if (!currencyCode) return numericAmount.toLocaleString(undefined, { maximumFractionDigits: 0 });
  return `${numericAmount.toLocaleString(undefined, { maximumFractionDigits: 0 })} ${currencyCode}`;
}

function formatPercent(value: unknown): string {
  const numericValue = numberValue(value);
  if (numericValue == null) return "-";
  return `${numericValue.toLocaleString(undefined, { maximumFractionDigits: 2 })}%`;
}

function formatRatio(value: unknown): string {
  const numericValue = numberValue(value);
  if (numericValue == null) return "-";
  return numericValue.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatPeriod(row: Record<string, unknown>): string {
  const start = dateValue(stringValue(row.period_start));
  const end = dateValue(stringValue(row.period_end));
  if (start === "-" && end === "-") return "-";
  if (start === "-") return end;
  if (end === "-") return start;
  return `${start} - ${end}`;
}

function metricLabel(value: string): string {
  return value.replace(/_/g, " ");
}

function FinancialAmountCell({
  row,
  metric,
}: {
  row: Record<string, unknown>;
  metric: string;
}) {
  const usd = formatUsdCents(row[`${metric}_usd_cents`]);
  const original = formatOriginalAmount(row[`${metric}_original_amount`], row.original_currency);
  return (
    <div className="flex min-w-32 flex-col gap-1">
      <span className="font-medium">{usd}</span>
      {original !== "-" && <span className="text-xs text-muted-foreground">{original}</span>}
    </div>
  );
}

function FinancialsSection({ rows }: { rows: Array<Record<string, unknown>> }) {
  const latest = rows[0];
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-3">
          <div>
            <CardTitle>Financials</CardTitle>
            <CardDescription>
              Annual financial statements stored for this BRREG source company.
            </CardDescription>
          </div>
          <Badge variant="outline">{rows.length.toLocaleString()}</Badge>
        </div>
      </CardHeader>
      <CardContent className="flex flex-col gap-5">
        {rows.length === 0 ? (
          <div className="py-6 text-sm text-muted-foreground">
            No financial statements stored for this BRREG company.
          </div>
        ) : (
          <>
            <KeyValueGrid
              items={[
                ["Latest fiscal year", latest?.fiscal_year],
                ["Statement type", metricLabel(textValue(latest?.statement_type))],
                ["Consolidated", latest?.is_consolidated],
                ["Currency", latest?.original_currency],
                ["Revenue", formatUsdCents(latest?.revenue_usd_cents)],
                ["Total assets", formatUsdCents(latest?.total_assets_usd_cents)],
                ["Net income", formatUsdCents(latest?.net_income_usd_cents)],
                ["Period", latest ? formatPeriod(latest) : undefined],
                ["FX date", dateValue(stringValue(latest?.fx_rate_date))],
              ]}
            />

            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Year</TableHead>
                    <TableHead>Period</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Revenue</TableHead>
                    <TableHead>Operating profit</TableHead>
                    <TableHead>Net income</TableHead>
                    <TableHead>Total assets</TableHead>
                    <TableHead>Equity</TableHead>
                    <TableHead>Liabilities</TableHead>
                    <TableHead>Employees</TableHead>
                    <TableHead>Ratios</TableHead>
                    <TableHead>Source</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((row, rowIndex) => (
                    <TableRow key={String(row.id ?? `${row.fiscal_year ?? "year"}-${rowIndex}`)}>
                      <TableCell className="font-medium">{textValue(row.fiscal_year)}</TableCell>
                      <TableCell>{formatPeriod(row)}</TableCell>
                      <TableCell>
                        <div className="flex flex-col gap-1">
                          <span>{metricLabel(textValue(row.statement_type))}</span>
                          {row.is_consolidated === true && (
                            <Badge variant="secondary">Consolidated</Badge>
                          )}
                        </div>
                      </TableCell>
                      <TableCell><FinancialAmountCell row={row} metric="revenue" /></TableCell>
                      <TableCell><FinancialAmountCell row={row} metric="operating_profit" /></TableCell>
                      <TableCell><FinancialAmountCell row={row} metric="net_income" /></TableCell>
                      <TableCell><FinancialAmountCell row={row} metric="total_assets" /></TableCell>
                      <TableCell><FinancialAmountCell row={row} metric="total_equity" /></TableCell>
                      <TableCell><FinancialAmountCell row={row} metric="total_liabilities" /></TableCell>
                      <TableCell>{textValue(row.employee_count)}</TableCell>
                      <TableCell>
                        <div className="flex min-w-36 flex-col gap-1 text-xs">
                          <span>Operating margin: {formatPercent(row.operating_margin_percent)}</span>
                          <span>Net margin: {formatPercent(row.net_margin_percent)}</span>
                          <span>Equity ratio: {formatPercent(row.equity_ratio_percent)}</span>
                          <span>Current ratio: {formatRatio(row.current_ratio)}</span>
                        </div>
                      </TableCell>
                      <TableCell>
                        {stringValue(row.source_url) ? (
                          <Button asChild variant="ghost" size="sm">
                            <a href={stringValue(row.source_url)} target="_blank" rel="noreferrer">
                              <ExternalLink data-icon="inline-start" />
                              Open
                            </a>
                          </Button>
                        ) : (
                          "-"
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}

function KeyValueGrid({
  items,
}: {
  items: Array<[string, unknown]>;
}) {
  return (
    <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {items.map(([label, value]) => (
        <div key={label} className="flex min-w-0 flex-col gap-1">
          <dt className="text-xs font-medium text-muted-foreground">{label}</dt>
          <dd className="break-words text-sm">{textValue(value)}</dd>
        </div>
      ))}
    </dl>
  );
}

function SimpleRowsTable({
  title,
  description,
  rows,
  columns,
}: {
  title: string;
  description: string;
  rows: Array<Record<string, unknown>>;
  columns: Array<[string, string]>;
}) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-3">
          <div>
            <CardTitle>{title}</CardTitle>
            <CardDescription>{description}</CardDescription>
          </div>
          <Badge variant="outline">{rows.length.toLocaleString()}</Badge>
        </div>
      </CardHeader>
      <CardContent>
        {rows.length === 0 ? (
          <div className="py-6 text-sm text-muted-foreground">No rows.</div>
        ) : (
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  {columns.map(([key, label]) => (
                    <TableHead key={key}>{label}</TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((row, rowIndex) => (
                  <TableRow key={String(row.id ?? rowIndex)}>
                    {columns.map(([key]) => (
                      <TableCell key={key}>{textValue(row[key])}</TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function JsonBlock({
  title,
  value,
}: {
  title: string;
  value: unknown;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <pre className="max-h-96 overflow-auto rounded-md bg-muted p-4 text-xs">
          {JSON.stringify(value ?? {}, null, 2)}
        </pre>
      </CardContent>
    </Card>
  );
}

function BrregSourceCompanyDetailView({
  company,
}: {
  company: BrregSourceCompanyDetail;
}) {
  const translationStatus = useMemo(() => {
    if (!company?.translation_status) return [];
    return Object.entries(company.translation_status).filter(([, value]) => value > 0);
  }, [company?.translation_status]);

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <Button asChild variant="ghost" size="sm">
          <Link to="/sources/brreg/source_entries">
            <ChevronLeft data-icon="inline-start" />
            Source entries
          </Link>
        </Button>
        <div className="flex flex-wrap gap-2">
          <Badge variant="outline">{company.lifecycle_status}</Badge>
          {company.registration_status && (
            <Badge variant="secondary">{company.registration_status}</Badge>
          )}
          <Badge variant="outline">{company.row_status}</Badge>
        </div>
      </div>

      <section className="flex flex-col gap-4">
        <div className="flex flex-col gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="text-2xl font-semibold tracking-normal">
              {company.organization_name}
            </h1>
            <span className="font-mono text-sm text-muted-foreground">
              {company.organization_number}
            </span>
          </div>
          <p className="max-w-4xl text-sm text-muted-foreground">
            {company.description_en ??
              company.description ??
              company.activity_description_en ??
              company.activity_description ??
              "No description available."}
          </p>
        </div>

        <KeyValueGrid
          items={[
            ["Organization form", company.organization_form_label_en ?? company.organization_form_label ?? company.organization_form_code],
            ["Country", company.country_iso2],
            ["Founded", dateValue(company.founded_date)],
            ["Employees", company.employee_count ?? company.employee_band],
            ["Last annual report", company.last_annual_report_year == null ? undefined : String(company.last_annual_report_year)],
            ["Updated", dateValue(company.updated_at)],
          ]}
        />
      </section>

      <Separator />

      <section className="flex flex-col gap-4">
        <h2 className="text-lg font-semibold">Registry Profile</h2>
        <KeyValueGrid
          items={[
            ["Unit registry date", dateValue(company.unit_registry_registered_at)],
            ["Enterprise registry date", dateValue(company.enterprise_registry_registered_at)],
            ["VAT registry date", dateValue(company.vat_registry_registered_at)],
            ["In VAT register", company.in_vat_register],
            ["In business register", company.in_business_register],
            ["Has registered employees", company.has_registered_employees],
            ["Bankrupt", company.is_bankrupt],
            ["Under liquidation", company.is_under_liquidation],
            ["Forced dissolution", company.is_forced_dissolution],
          ]}
        />
      </section>

      <SimpleRowsTable
        title="Industries"
        description="BRREG source classifications and mapped NACE values."
        rows={rowsFrom(company.industries)}
        columns={[
          ["classification_type", "Type"],
          ["source_code", "Source code"],
          ["source_label_en", "Label EN"],
          ["source_label", "Label"],
          ["mapped_nace_code", "NACE"],
          ["mapping_confidence", "Confidence"],
        ]}
      />

      <SimpleRowsTable
        title="Addresses"
        description="Business and postal addresses from the source profile."
        rows={rowsFrom(company.addresses)}
        columns={[
          ["address_type", "Type"],
          ["formatted_address", "Address"],
          ["city", "City"],
          ["municipality", "Municipality"],
          ["postal_code", "Postal code"],
          ["geocode_status", "Geocode"],
        ]}
      />

      <SimpleRowsTable
        title="Websites"
        description="Websites associated with this BRREG source company."
        rows={rowsFrom(company.websites)}
        columns={[
          ["url", "URL"],
          ["website_type", "Type"],
          ["source", "Source"],
          ["status", "Status"],
          ["confidence", "Confidence"],
        ]}
      />

      <SimpleRowsTable
        title="Domains"
        description="Domains associated with this BRREG source company."
        rows={rowsFrom(company.domains)}
        columns={[
          ["domain", "Domain"],
          ["domain_type", "Type"],
          ["source", "Source"],
          ["status", "Status"],
          ["confidence", "Confidence"],
        ]}
      />

      <SimpleRowsTable
        title="Contacts"
        description="Phone, email, and other contact values from the profile."
        rows={rowsFrom(company.contacts)}
        columns={[
          ["contact_type", "Type"],
          ["value", "Value"],
          ["label_en", "Label EN"],
          ["source", "Source"],
          ["status", "Status"],
        ]}
      />

      <FinancialsSection rows={rowsFrom(company.financial_years)} />

      <Card>
        <CardHeader>
          <CardTitle>Translation Tasks</CardTitle>
          <CardDescription>Source-field translation status for this company.</CardDescription>
        </CardHeader>
        <CardContent>
          {translationStatus.length === 0 ? (
            <div className="text-sm text-muted-foreground">No translation task state.</div>
          ) : (
            <div className="flex flex-wrap gap-2">
              {translationStatus.map(([status, count]) => (
                <Badge key={status} variant="outline">
                  {status.replace(/_/g, " ")}: {count.toLocaleString()}
                </Badge>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <JsonBlock title="Evidence" value={company.evidence} />
        <JsonBlock title="Metadata" value={company.metadata} />
      </div>
    </div>
  );
}

function AriregisterSourceCompanyDetailView({
  company,
}: {
  company: AriregisterSourceCompanyDetail;
}) {
  const registryURL = `https://ariregister.rik.ee/est/company/${company.registry_code}`;
  const structuredRows = {
    names: rowsFrom(company.names),
    statuses: rowsFrom(company.statuses),
    legal_forms: rowsFrom(company.legal_forms),
    addresses: rowsFrom(company.addresses),
    contacts: rowsFrom(company.contacts),
    websites: rowsFrom(company.websites),
    domains: rowsFrom(company.domains),
    industries: rowsFrom(company.industries),
    capital: rowsFrom(company.capital),
    annual_reports: rowsFrom(company.annual_reports),
    articles: rowsFrom(company.articles),
    registry_notes: rowsFrom(company.registry_notes),
  };

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <Button asChild variant="ghost" size="sm">
          <Link to="/sources/ariregister/source_entries">
            <ChevronLeft data-icon="inline-start" />
            Source entries
          </Link>
        </Button>
        <div className="flex flex-wrap gap-2">
          <Badge variant="outline">{company.lifecycle_status}</Badge>
          {company.registration_status_label && (
            <Badge variant="secondary">{company.registration_status_label}</Badge>
          )}
          <Badge variant="outline">{company.row_status}</Badge>
        </div>
      </div>

      <section className="flex flex-col gap-4">
        <div className="flex flex-col gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="text-2xl font-semibold tracking-normal">
              {company.legal_name}
            </h1>
            <span className="font-mono text-sm text-muted-foreground">
              {company.registry_code}
            </span>
          </div>
          <div className="flex flex-wrap items-center gap-3 text-sm text-muted-foreground">
            <span>{company.legal_name_en ?? company.legal_name_normalized}</span>
            <Button asChild variant="ghost" size="sm" className="h-7 px-2">
              <a href={registryURL} target="_blank" rel="noreferrer">
                <ExternalLink data-icon="inline-start" />
                Ariregister
              </a>
            </Button>
          </div>
        </div>

        <KeyValueGrid
          items={[
            ["Legal form", company.legal_form_label_en ?? company.legal_form_label ?? company.legal_form_code],
            ["Subtype", company.legal_form_subtype_label_en ?? company.legal_form_subtype_label ?? company.legal_form_subtype],
            ["Registration", company.registration_status_label_en ?? company.registration_status_label ?? company.registration_status],
            ["Active label", company.active_label_en ?? company.active_label],
            ["Country", company.country_iso2],
            ["Region", company.region_label_long_en ?? company.region_label_long ?? company.region_label],
            ["First registered", dateValue(company.first_registered_on)],
            ["Deleted", dateValue(company.deleted_on)],
            ["EVKS registered", dateValue(company.evks_registered_at)],
            ["Employees", company.employee_count ?? company.employee_band],
            ["Last annual report", company.last_annual_report_year],
            ["Source updated", dateValue(company.source_updated_at)],
          ]}
        />
      </section>

      <Separator />

      <section className="flex flex-col gap-4">
        <h2 className="text-lg font-semibold">Registry Profile</h2>
        <KeyValueGrid
          items={[
            ["Is active", company.is_active],
            ["Accounting required", company.is_accounting_required],
            ["Reports beneficial owners", company.reports_beneficial_owners],
            ["Missing beneficial owner notice", company.has_missing_beneficial_owner_discrepancy_notice],
            ["Founded without contribution", company.founded_without_contribution],
            ["Waived form requirements", company.waived_form_requirements],
            ["Employee source", company.employee_count_source],
            ["Profile version", company.profile_version],
            ["Updated", dateValue(company.updated_at)],
          ]}
        />
      </section>

      <SimpleRowsTable
        title="Names"
        description="Registered source names and name history."
        rows={structuredRows.names}
        columns={[
          ["name", "Name"],
          ["name_en", "Name EN"],
          ["started_on", "Started"],
          ["ended_on", "Ended"],
          ["entry_number", "Entry"],
        ]}
      />

      <SimpleRowsTable
        title="Statuses"
        description="Ariregister status records and source status labels."
        rows={structuredRows.statuses}
        columns={[
          ["status_code", "Code"],
          ["status_label_en", "Label EN"],
          ["status_label", "Label"],
          ["started_on", "Started"],
          ["entry_number", "Entry"],
        ]}
      />

      <SimpleRowsTable
        title="Legal Forms"
        description="Legal form records, subtype values, and source labels."
        rows={structuredRows.legal_forms}
        columns={[
          ["legal_form_code", "Code"],
          ["legal_form_label_en", "Label EN"],
          ["legal_form_label", "Label"],
          ["legal_form_subtype_label_en", "Subtype EN"],
          ["started_on", "Started"],
        ]}
      />

      <SimpleRowsTable
        title="Industries"
        description="Declared Ariregister activities and mapped NACE values."
        rows={structuredRows.industries}
        columns={[
          ["classification_type", "Type"],
          ["emtak_code", "EMTAK"],
          ["emtak_label_en", "Label EN"],
          ["emtak_label", "Label"],
          ["nace_code", "NACE"],
          ["is_primary", "Primary"],
          ["started_on", "Started"],
        ]}
      />

      <SimpleRowsTable
        title="Addresses"
        description="Registered addresses and normalized ADS address values."
        rows={structuredRows.addresses}
        columns={[
          ["address_type", "Type"],
          ["normalized_full_address", "Address"],
          ["street_text", "Street"],
          ["ehak_name", "EHAK"],
          ["postal_code", "Postal code"],
          ["country_label", "Country"],
          ["started_on", "Started"],
        ]}
      />

      <SimpleRowsTable
        title="Contacts"
        description="Email, phone, and other contact values from Ariregister."
        rows={structuredRows.contacts}
        columns={[
          ["contact_type", "Type"],
          ["value", "Value"],
          ["normalized_value", "Normalized"],
          ["contact_type_label_en", "Label EN"],
          ["contact_type_label", "Label"],
          ["is_primary", "Primary"],
          ["status", "Status"],
        ]}
      />

      <SimpleRowsTable
        title="Websites"
        description="Websites associated with this Ariregister source company."
        rows={structuredRows.websites}
        columns={[
          ["url", "URL"],
          ["website_type", "Type"],
          ["source", "Source"],
          ["status", "Status"],
          ["confidence", "Confidence"],
        ]}
      />

      <SimpleRowsTable
        title="Domains"
        description="Domains associated with this Ariregister source company."
        rows={structuredRows.domains}
        columns={[
          ["domain", "Domain"],
          ["domain_type", "Type"],
          ["source", "Source"],
          ["status", "Status"],
          ["confidence", "Confidence"],
        ]}
      />

      <SimpleRowsTable
        title="Capital"
        description="Share capital records and source currency values."
        rows={structuredRows.capital}
        columns={[
          ["capital_amount", "Amount"],
          ["capital_currency", "Currency"],
          ["capital_currency_label_en", "Currency EN"],
          ["capital_currency_label", "Currency label"],
          ["introduced_on", "Introduced"],
          ["ended_on", "Ended"],
        ]}
      />

      <SimpleRowsTable
        title="Annual Reports"
        description="Annual report metadata and reported employee/activity values."
        rows={structuredRows.annual_reports}
        columns={[
          ["fiscal_year", "Year"],
          ["period_start", "Period start"],
          ["period_end", "Period end"],
          ["employee_count", "Employees"],
          ["activity_emtak_code", "EMTAK"],
          ["activity_label_en", "Activity EN"],
          ["activity_label", "Activity"],
        ]}
      />

      <SimpleRowsTable
        title="Articles"
        description="Articles of association records and related source flags."
        rows={structuredRows.articles}
        columns={[
          ["started_on", "Started"],
          ["confirmed_on", "Confirmed"],
          ["changed_on", "Changed"],
          ["contains_special_rights", "Special rights"],
          ["explanation_en", "Explanation EN"],
          ["explanation", "Explanation"],
        ]}
      />

      <SimpleRowsTable
        title="Registry Notes"
        description="Registry notes and notices parsed from the source payload."
        rows={structuredRows.registry_notes}
        columns={[
          ["note_type", "Type"],
          ["note_label_en", "Label EN"],
          ["note_label", "Label"],
          ["note_text_en", "Text EN"],
          ["note_text", "Text"],
          ["started_on", "Started"],
        ]}
      />

      <div className="grid gap-4 lg:grid-cols-2">
        <JsonBlock title="Structured Rows JSON" value={structuredRows} />
        <JsonBlock title="Normalized Payload" value={company.normalized_payload} />
        <JsonBlock title="Raw Company Payload" value={company.raw_company_payload} />
        <JsonBlock title="Evidence" value={company.evidence} />
        <JsonBlock title="Metadata" value={company.metadata} />
      </div>
    </div>
  );
}

type SourceCompanyDetail =
  | BrregSourceCompanyDetail
  | AriregisterSourceCompanyDetail;

export default function SourceCompanyDetailPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  const { companyId } = useParams<{ companyId: string }>();
  const [company, setCompany] = useState<SourceCompanyDetail>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const supportedSource =
    source.name === "brreg" || source.name === "ariregister";

  useEffect(() => {
    if (!companyId || !supportedSource) {
      setLoading(false);
      return;
    }
    let ignore = false;
    setLoading(true);
    setError(undefined);
    setCompany(undefined);

    const detailRequest =
      source.name === "ariregister"
        ? api.getAriregisterSourceCompanyDetail(companyId)
        : api.getBrregSourceCompanyDetail(companyId);

    detailRequest
      .then((detail) => {
        if (!ignore) setCompany(detail);
      })
      .catch((err) => {
        if (!ignore) {
          const label = source.name === "ariregister" ? "Ariregister" : "BRREG";
          setError(errorMessage(err, `Failed to load ${label} source company.`));
        }
      })
      .finally(() => {
        if (!ignore) setLoading(false);
      });
    return () => {
      ignore = true;
    };
  }, [companyId, source.name, supportedSource]);

  if (!supportedSource) {
    return (
      <Alert>
        <AlertDescription>
          Source company details are available for BRREG and Ariregister only.
        </AlertDescription>
      </Alert>
    );
  }

  if (loading) {
    return (
      <div className="flex flex-col gap-4">
        <Skeleton className="h-24 w-full" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  if (error || !company) {
    const label = source.name === "ariregister" ? "Ariregister" : "BRREG";
    return (
      <Alert variant="destructive">
        <AlertDescription>
          {error ?? `${label} source company not found.`}
        </AlertDescription>
      </Alert>
    );
  }

  if (source.name === "ariregister") {
    return (
      <AriregisterSourceCompanyDetailView
        company={company as AriregisterSourceCompanyDetail}
      />
    );
  }

  return (
    <BrregSourceCompanyDetailView company={company as BrregSourceCompanyDetail} />
  );
}
