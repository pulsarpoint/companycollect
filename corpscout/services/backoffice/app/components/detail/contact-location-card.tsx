import { useEffect, useRef } from "react";
import { Link, useFetcher } from "react-router";
import {
  Building2,
  CircleAlert,
  Globe,
  Mail,
  Mailbox,
  MapPin,
  MapPinOff,
  Phone,
  Printer,
  Smartphone,
} from "lucide-react";
import type { CountryConfig } from "~/lib/countries";
import type {
  AddressRow,
  AddressSourceMember,
  ContactRow,
} from "~/lib/queries.server";
import type { SameAddressCompaniesResult } from "~/lib/address-companies.server";
import { humanizeFieldKey } from "~/components/detail/fields";
import { Badge } from "~/components/ui/badge";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "~/components/ui/accordion";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import { Separator } from "~/components/ui/separator";
import { MiniMap } from "~/components/detail/mini-map";

const CONTACT_ICONS: Record<string, typeof Mail> = {
  email: Mail,
  phone: Phone,
  mobile: Smartphone,
  fax: Printer,
  website: Globe,
};

interface StoredAddressGeocode {
  lat: number;
  lon: number;
  provider?: string;
  precision?: string;
  matchMethod?: string;
  matchConfidence?: number;
  sourceRecordId?: string;
  sourceRecordUrl?: string;
  sourceSnapshotAt?: string;
  coordinateLocality?: string;
  coordinateSupportingPointCount?: number;
}

export interface AddressGeocodeOutcomeCopy {
  title: string;
  description: string;
  badge: string;
}

export interface AddressGeocodeEvidenceLink {
  url: string;
  label: string;
}

export function addressGeocodeEvidenceLink(
  address: AddressRow,
): AddressGeocodeEvidenceLink | null {
  if (address.geocode_source_record_url) {
    return {
      url: address.geocode_source_record_url,
      label: "Matched OpenStreetMap record",
    };
  }
  if (address.geocode_source_url) {
    return {
      url: address.geocode_source_url,
      label: "OpenStreetMap extract provenance",
    };
  }
  return null;
}

function approximateCityLocationDescription(
  geocode: StoredAddressGeocode,
): string {
  const pointCount = geocode.coordinateSupportingPointCount
    ? ` ${new Intl.NumberFormat("en").format(
        geocode.coordinateSupportingPointCount,
      )}`
    : "";
  const locality = geocode.coordinateLocality
    ? ` tagged ${geocode.coordinateLocality}`
    : "";
  return (
    `The marker is the median location of${pointCount} OpenStreetMap ` +
    `address points${locality}. It is not the company's building.`
  );
}

export function addressGeocodeOutcomeCopy(
  address: AddressRow,
): AddressGeocodeOutcomeCopy | null {
  switch (address.geocode_status) {
    case "matched_exact":
      return {
        title: "Exact building location found",
        description:
          address.geocode_match_method === "city_street_house_exact_unique"
            ? "City, street, and house number matched one OpenStreetMap record."
            : "Postal code, street, and house number matched one OpenStreetMap record.",
        badge: "Exact match",
      };
    case "postal_box":
      return {
        title: "Postal address only",
        description:
          address.geocode_precision === "city" &&
          address.geocode_coordinate_locality
            ? `This registry address is a PO box, so it does not identify a physical building. The map shows an approximate location for ${address.geocode_coordinate_locality}.`
            : "This registry address is a PO box, so it does not identify a physical building.",
        badge: "PO box",
      };
    case "ambiguous": {
      const candidates = address.geocode_candidate_count ?? 0;
      return {
        title: "Multiple possible building locations",
        description: `${candidates} OpenStreetMap records match this address, so no coordinate was selected.`,
        badge: "Ambiguous",
      };
    }
    case "unmatched":
      return {
        title: "No exact building match",
        description:
          "The Sweden OpenStreetMap extract has no building matching this street and house number within its postal code or city.",
        badge: "Unmatched",
      };
    case "invalid_address":
      return {
        title: "Insufficient address details",
        description:
          "The registry address lacks the structured postal code, street, or house number required for an exact match.",
        badge: "Incomplete",
      };
    case "foreign_address":
      return {
        title: "Outside the Sweden address index",
        description:
          "This foreign registry address was not matched against the Sweden OpenStreetMap extract.",
        badge: "Foreign",
      };
    default:
      return null;
  }
}

export function AddressGeocodeOutcomeNotice({
  address,
}: {
  address: AddressRow;
}) {
  const copy = addressGeocodeOutcomeCopy(address);
  if (!copy) return null;
  const Icon =
    address.geocode_status === "matched_exact"
      ? MapPin
      : address.geocode_status === "postal_box"
        ? Mailbox
        : address.geocode_status === "ambiguous"
          ? CircleAlert
          : MapPinOff;
  const candidateUrls =
    address.geocode_status === "ambiguous"
      ? (address.geocode_candidate_record_urls ?? [])
      : [];
  const evidenceLink = addressGeocodeEvidenceLink(address);
  const snapshotDate = address.geocode_source_snapshot_at
    ? new Intl.DateTimeFormat("en", { dateStyle: "medium" }).format(
        new Date(address.geocode_source_snapshot_at),
      )
    : null;

  return (
    <Alert>
      <Icon />
      <AlertTitle className="flex flex-wrap items-center gap-2">
        {copy.title}
        <Badge
          variant={
            address.geocode_status === "matched_exact" ? "secondary" : "outline"
          }
        >
          {copy.badge}
        </Badge>
      </AlertTitle>
      <AlertDescription className="flex flex-col gap-1.5">
        <p>{copy.description}</p>
        {snapshotDate ? (
          <p>Checked against the OpenStreetMap snapshot from {snapshotDate}.</p>
        ) : null}
        {address.geocode_provider === "openstreetmap" ? (
          <p className="flex flex-wrap items-center gap-2">
            <span>Geocoding source: OpenStreetMap.</span>
            {evidenceLink ? (
              <a
                href={evidenceLink.url}
                target="_blank"
                rel="noreferrer"
              >
                {evidenceLink.label}
              </a>
            ) : null}
          </p>
        ) : null}
        {candidateUrls.length > 0 ? (
          <p className="flex flex-wrap gap-2">
            {candidateUrls.map((url, index) => (
              <a key={url} href={url} target="_blank" rel="noreferrer">
                Candidate {index + 1}
              </a>
            ))}
          </p>
        ) : null}
        {address.geocode_source_run_id ? (
          <p className="font-mono text-xs">
            Match run {address.geocode_source_run_id}
          </p>
        ) : null}
      </AlertDescription>
    </Alert>
  );
}

function sourceObservedDate(value: string): string {
  const normalized = /(?:Z|[+-]\d{2}:?\d{2})$/.test(value)
    ? value
    : `${value.replace(" ", "T")}Z`;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en", { dateStyle: "medium" }).format(date);
}

export function AddressSourceEvidence({
  members,
}: {
  members: AddressSourceMember[];
}) {
  if (members.length === 0) return null;
  return (
    <Accordion>
      <AccordionItem value="source-records">
        <AccordionTrigger>
          Source records ({new Intl.NumberFormat("en").format(members.length)})
        </AccordionTrigger>
        <AccordionContent>
          <ul className="flex flex-col gap-3">
            {members.map((member) => (
              <li
                key={`${member.address_source}-${member.address_key}`}
                className="flex flex-col gap-1.5"
              >
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="secondary">
                    {humanizeFieldKey(member.address_source)}
                  </Badge>
                  <Badge variant="outline">
                    {humanizeFieldKey(member.address_type)}
                  </Badge>
                  <span className="text-muted-foreground text-xs">
                    Observed {sourceObservedDate(member.source_observed_at)}
                  </span>
                </div>
                <p>{member.display_address}</p>
                {member.raw_address &&
                member.raw_address !== member.display_address ? (
                  <p className="text-muted-foreground break-all font-mono text-xs">
                    Source value: {member.raw_address}
                  </p>
                ) : null}
                <p className="text-muted-foreground break-all font-mono text-xs">
                  Record {member.registry_source_record_uid}
                </p>
              </li>
            ))}
          </ul>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
}

export function storedAddressGeocode(
  record: Record<string, unknown>,
  addresses: AddressRow[],
): StoredAddressGeocode | null {
  const lat = record.address_latitude;
  const lon = record.address_longitude;
  if (typeof lat === "number" && typeof lon === "number") return { lat, lon };
  const address = addresses.find(
    (candidate) =>
      typeof candidate.latitude === "number" &&
      typeof candidate.longitude === "number",
  );
  if (!address) return null;
  return {
    lat: address.latitude as number,
    lon: address.longitude as number,
    provider: address.geocode_provider,
    precision: address.geocode_precision,
    matchMethod: address.geocode_match_method,
    matchConfidence: address.geocode_match_confidence,
    sourceRecordId: address.geocode_source_record_id,
    sourceRecordUrl: address.geocode_source_record_url,
    sourceSnapshotAt: address.geocode_source_snapshot_at,
    coordinateLocality: address.geocode_coordinate_locality,
    coordinateSupportingPointCount:
      address.geocode_coordinate_supporting_point_count,
  };
}

function isForeignAddress(address: AddressRow): boolean {
  return (
    address.address_is_foreign === true || address.address_is_foreign === 1
  );
}

function normalizedCountryCode(address: AddressRow): string | null {
  const countryCode = address.address_country_code?.trim().toUpperCase() ?? "";
  return /^[A-Z]{2}$/.test(countryCode) ? countryCode : null;
}

function countryFlag(countryCode: string): string {
  return [...countryCode]
    .map((letter) => String.fromCodePoint(127397 + letter.charCodeAt(0)))
    .join("");
}

export function foreignAddressBadgeText(
  address: AddressRow,
  registerCountryName: string,
): string | null {
  if (!isForeignAddress(address)) return null;
  const countryCode = normalizedCountryCode(address);
  if (!countryCode) return `🌐 Address outside ${registerCountryName}`;
  const countryName = new Intl.DisplayNames(["en"], { type: "region" }).of(
    countryCode,
  );
  return `${countryFlag(countryCode)} Address in ${countryName ?? countryCode}`;
}

export function geocodeCountryCodeForAddress(
  address: AddressRow,
  registerCountryCode: string,
): string {
  return (
    normalizedCountryCode(address)?.toLowerCase() ??
    (isForeignAddress(address) ? "" : registerCountryCode)
  );
}

export function ContactLocationCard({
  country,
  companyId,
  contacts,
  addresses,
  record,
}: {
  country: CountryConfig;
  companyId: string;
  contacts: ContactRow[];
  addresses: AddressRow[];
  record: Record<string, unknown>;
}) {
  const fetcher = useFetcher<{
    coords: { lat: number; lon: number } | null;
    precision: "address" | "street" | null;
  }>();
  const sameAddressFetcher = useFetcher<SameAddressCompaniesResult>();
  const requested = useRef(false);
  const stored = storedAddressGeocode(record, addresses);
  const realAddresses = addresses.filter((a) => a.full_address.trim() !== "");
  const geocodeAddress = realAddresses[0];
  // geocode_address where a register publishes one: the display address is the
  // full postal form, which is what a reader wants and not always what a
  // geocoder can resolve. Brazil's carries a building complement and a
  // zero-padded street number that make Nominatim return nothing.
  const candidateTarget =
    !stored && country.code !== "se" && realAddresses.length > 0
      ? geocodeAddress.geocode_address || geocodeAddress.full_address
      : null;
  // Over-long addresses are unresolvable: no fetch, no map — same as no address at all.
  const geocodeTarget =
    candidateTarget && candidateTarget.length <= 300 ? candidateTarget : null;
  const canSearchSameAddress =
    country.code === "se" &&
    geocodeAddress !== undefined &&
    Boolean(geocodeAddress.geocode_street) &&
    Boolean(geocodeAddress.geocode_postal_code) &&
    !isForeignAddress(geocodeAddress);

  useEffect(() => {
    if (
      geocodeTarget &&
      !requested.current &&
      fetcher.state === "idle" &&
      fetcher.data === undefined
    ) {
      requested.current = true;
      const searchParams = new URLSearchParams({
        address: geocodeTarget,
        countryCode: geocodeCountryCodeForAddress(geocodeAddress, country.code),
      });
      if (geocodeAddress.geocode_street) {
        searchParams.set("fallbackStreet", geocodeAddress.geocode_street);
        searchParams.set(
          "fallbackPostalCode",
          geocodeAddress.geocode_postal_code ?? "",
        );
      }
      fetcher.load(
        `/company/${country.code}/geocode?${searchParams.toString()}`,
      );
    }
  }, [geocodeTarget, fetcher, country.code]);

  if (contacts.length === 0 && realAddresses.length === 0) return null;
  const coords = stored ?? fetcher.data?.coords ?? null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Contact &amp; location</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {contacts.length > 0 ? (
          <ul className="flex flex-col gap-1.5">
            {contacts.map((c, i) => {
              const Icon = CONTACT_ICONS[c.contact_type];
              const isLink =
                c.contact_type === "website" ||
                c.contact_value.startsWith("http");
              return (
                <li
                  key={`${c.contact_type}-${c.contact_value}-${i}`}
                  className="flex items-baseline gap-2 text-sm"
                >
                  {Icon ? (
                    <Icon className="text-muted-foreground size-3.5 shrink-0 self-center" />
                  ) : (
                    <Badge variant="outline">{c.contact_type}</Badge>
                  )}
                  {isLink ? (
                    <a
                      href={
                        c.contact_value.startsWith("http")
                          ? c.contact_value
                          : `https://${c.contact_value}`
                      }
                      target="_blank"
                      rel="noreferrer"
                      className="break-all underline underline-offset-2"
                    >
                      {c.contact_value}
                    </a>
                  ) : (
                    <span className="break-all">{c.contact_value}</span>
                  )}
                </li>
              );
            })}
          </ul>
        ) : null}

        {realAddresses.map((a, i) => {
          const foreignAddressBadge = foreignAddressBadgeText(a, country.name);
          const addressTypes =
            a.address_types && a.address_types.length > 0
              ? a.address_types
              : [a.address_type];
          return (
            <div
              key={`${a.address_type}-${i}`}
              className="flex flex-col gap-1 text-sm"
            >
              <div className="flex flex-wrap items-center gap-2">
                {addressTypes.map((addressType) => (
                  <Badge key={addressType} variant="outline">
                    {humanizeFieldKey(addressType)}
                  </Badge>
                ))}
                {(a.address_sources?.length ?? 0) > 1 ? (
                  <Badge variant="secondary">
                    {a.address_sources?.length} sources
                  </Badge>
                ) : null}
                {foreignAddressBadge ? (
                  <Badge variant="outline">{foreignAddressBadge}</Badge>
                ) : null}
              </div>
              <p>{a.full_address}</p>
              {country.code === "se" ? (
                <AddressGeocodeOutcomeNotice address={a} />
              ) : null}
              <AddressSourceEvidence members={a.source_members ?? []} />
            </div>
          );
        })}

        {coords ? (
          <div className="flex flex-col gap-2">
            {stored?.provider === "openstreetmap" &&
            stored.precision === "building" ? (
              <div className="flex flex-col items-start gap-1.5">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge className="w-fit" variant="secondary">
                    Exact OpenStreetMap address match
                  </Badge>
                  {stored.sourceRecordUrl ? (
                    <a
                      href={stored.sourceRecordUrl}
                      target="_blank"
                      rel="noreferrer"
                      className="text-sm underline underline-offset-2"
                    >
                      View OSM evidence
                    </a>
                  ) : null}
                </div>
                <p className="text-muted-foreground text-xs">
                  Postal code, street, and house number matched one OSM record
                  {stored.sourceSnapshotAt
                    ? ` from the ${new Intl.DateTimeFormat("en", {
                        dateStyle: "medium",
                      }).format(new Date(stored.sourceSnapshotAt))} snapshot.`
                    : "."}
                </p>
              </div>
            ) : null}
            {stored?.provider === "openstreetmap" &&
            stored.precision === "city" ? (
              <div className="flex flex-col items-start gap-1.5">
                <Badge className="w-fit" variant="outline">
                  Approximate city location
                </Badge>
                <p className="text-muted-foreground text-xs">
                  {approximateCityLocationDescription(stored)}
                </p>
              </div>
            ) : null}
            {!stored && fetcher.data?.precision === "street" ? (
              <Badge className="w-fit" variant="outline">
                Approximate street location
              </Badge>
            ) : null}
            <MiniMap lat={coords.lat} lon={coords.lon} />
          </div>
        ) : geocodeTarget && fetcher.state !== "idle" ? (
          <div className="bg-muted text-muted-foreground flex h-48 w-full items-center justify-center rounded-md text-xs">
            Locating…
          </div>
        ) : null}

        {canSearchSameAddress ? (
          <>
            <Separator />
            <section className="flex flex-col gap-3">
              {sameAddressFetcher.data ? (
                <>
                  <div className="flex flex-wrap items-baseline gap-2">
                    <h3 className="text-sm font-medium">
                      Other companies at this address
                    </h3>
                    <Badge variant="secondary">
                      {sameAddressFetcher.data.companies.length}
                    </Badge>
                  </div>
                  <p className="text-muted-foreground text-xs">
                    Matched by normalized street, postal code, and country;
                    care-of and floor are ignored.
                  </p>
                  {sameAddressFetcher.data.companies.length > 0 ? (
                    <ul className="flex flex-col gap-2">
                      {sameAddressFetcher.data.companies.map((company) => (
                        <li
                          key={company.company_id}
                          className="flex flex-wrap items-baseline justify-between gap-2"
                        >
                          <div className="min-w-0">
                            <Link
                              to={`/company/${country.code}/${encodeURIComponent(company.company_id)}`}
                              className="font-medium hover:underline"
                            >
                              {company.company_name || company.company_id}
                            </Link>
                            <p className="text-muted-foreground font-mono text-xs">
                              {company.company_id}
                            </p>
                          </div>
                          <Badge variant="outline">{company.status}</Badge>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <Empty className="p-4">
                      <EmptyHeader>
                        <EmptyMedia variant="icon">
                          <Building2 />
                        </EmptyMedia>
                        <EmptyTitle>No other companies found</EmptyTitle>
                        <EmptyDescription>
                          No other Swedish registration uses this normalized
                          building address.
                        </EmptyDescription>
                      </EmptyHeader>
                    </Empty>
                  )}
                  {sameAddressFetcher.data.truncated ? (
                    <p className="text-muted-foreground text-xs">
                      Showing the first 50 matches.
                    </p>
                  ) : null}
                </>
              ) : (
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <h3 className="text-sm font-medium">
                      Companies at this address
                    </h3>
                    <p className="text-muted-foreground text-xs">
                      Find other registrations in the same building.
                    </p>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={sameAddressFetcher.state !== "idle"}
                    onClick={() =>
                      sameAddressFetcher.load(
                        `/company/${country.code}/${encodeURIComponent(companyId)}/same-address`,
                      )
                    }
                  >
                    <Building2 data-icon="inline-start" />
                    {sameAddressFetcher.state === "idle"
                      ? "Show companies"
                      : "Finding…"}
                  </Button>
                </div>
              )}
            </section>
          </>
        ) : null}
      </CardContent>
    </Card>
  );
}
