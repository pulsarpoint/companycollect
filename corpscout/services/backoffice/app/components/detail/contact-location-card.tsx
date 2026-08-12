import { useEffect, useRef } from "react";
import { Link, useFetcher } from "react-router";
import {
  Building2,
  Globe,
  Mail,
  Phone,
  Printer,
  Smartphone,
} from "lucide-react";
import type { CountryConfig } from "~/lib/countries";
import type { AddressRow, ContactRow } from "~/lib/queries.server";
import type { SameAddressCompaniesResult } from "~/lib/address-companies.server";
import { humanizeFieldKey } from "~/components/detail/fields";
import { Badge } from "~/components/ui/badge";
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

function storedCoords(
  record: Record<string, unknown>,
  addresses: AddressRow[],
): { lat: number; lon: number } | null {
  const lat = record.address_latitude;
  const lon = record.address_longitude;
  if (typeof lat === "number" && typeof lon === "number") return { lat, lon };
  const address = addresses.find(
    (candidate) =>
      typeof candidate.latitude === "number" &&
      typeof candidate.longitude === "number",
  );
  return address ? { lat: address.latitude!, lon: address.longitude! } : null;
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
  const stored = storedCoords(record, addresses);
  const realAddresses = addresses.filter((a) => a.full_address.trim() !== "");
  const geocodeAddress = realAddresses[0];
  // geocode_address where a register publishes one: the display address is the
  // full postal form, which is what a reader wants and not always what a
  // geocoder can resolve. Brazil's carries a building complement and a
  // zero-padded street number that make Nominatim return nothing.
  const candidateTarget =
    !stored && realAddresses.length > 0
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
          return (
            <div
              key={`${a.address_type}-${i}`}
              className="flex flex-col gap-1 text-sm"
            >
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-muted-foreground text-xs font-medium uppercase tracking-wide">
                  {humanizeFieldKey(a.address_type)}
                </p>
                {foreignAddressBadge ? (
                  <Badge variant="outline">{foreignAddressBadge}</Badge>
                ) : null}
              </div>
              <p>{a.full_address}</p>
            </div>
          );
        })}

        {coords ? (
          <div className="flex flex-col gap-2">
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
