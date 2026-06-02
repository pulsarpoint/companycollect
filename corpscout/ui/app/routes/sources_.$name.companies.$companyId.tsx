import { useEffect, useMemo, useState } from "react";
import { Link, useOutletContext, useParams } from "react-router";
import { ChevronLeft } from "lucide-react";
import { api, errorMessage } from "~/lib/api";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import type { BrregSourceCompanyDetail } from "~/types/api";
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

function dateValue(value?: string): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString();
}

function rowsFrom(value: Array<Record<string, unknown>> | undefined) {
  return Array.isArray(value) ? value : [];
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
  value: Record<string, unknown>;
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

export default function BrregSourceCompanyDetailPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  const { companyId } = useParams<{ companyId: string }>();
  const [company, setCompany] = useState<BrregSourceCompanyDetail>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();

  useEffect(() => {
    if (!companyId || source.name !== "brreg") return;
    let ignore = false;
    setLoading(true);
    setError(undefined);
    setCompany(undefined);
    api
      .getBrregSourceCompanyDetail(companyId)
      .then((detail) => {
        if (!ignore) setCompany(detail);
      })
      .catch((err) => {
        if (!ignore) setError(errorMessage(err, "Failed to load BRREG source company."));
      })
      .finally(() => {
        if (!ignore) setLoading(false);
      });
    return () => {
      ignore = true;
    };
  }, [companyId, source.name]);

  const translationStatus = useMemo(() => {
    if (!company?.translation_status) return [];
    return Object.entries(company.translation_status).filter(([, value]) => value > 0);
  }, [company?.translation_status]);

  if (source.name !== "brreg") {
    return (
      <Alert>
        <AlertDescription>Source company details are available for BRREG only.</AlertDescription>
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
    return (
      <Alert variant="destructive">
        <AlertDescription>{error ?? "BRREG source company not found."}</AlertDescription>
      </Alert>
    );
  }

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
