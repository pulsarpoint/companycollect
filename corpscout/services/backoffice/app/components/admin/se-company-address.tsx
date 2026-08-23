import { MapPinIcon } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import {
  DefinitionList,
  EMPTY_VALUE,
  text,
} from "~/components/admin/definition-list";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "~/components/ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "~/components/ui/empty";
import type { SeCompanyAddressRow } from "~/lib/se-company-address.server";

/** The register's own address-type keys, spelled for a reader. An unknown key
 * falls through unchanged rather than being hidden. */
const ADDRESS_TYPE_LABELS: Record<string, string> = {
  postal: "Postal address",
  visiting: "Visiting address",
  visiting_or_postal: "Visiting or postal address",
};

function addressTypeLabel(type: string): string {
  return ADDRESS_TYPE_LABELS[type] ?? type;
}

/** OpenStreetMap at the geocoded point. Zoom 18 is building level, which is
 * what a `matched_exact` / `building` geocode claims to be. */
function openStreetMapHref(latitude: string, longitude: string): string {
  return `https://www.openstreetmap.org/?mlat=${latitude}&mlon=${longitude}#map=18/${latitude}/${longitude}`;
}

function AddressCard({ row }: { row: SeCompanyAddressRow }) {
  const hasPoint = row.latitude !== "" && row.longitude !== "";
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <CardTitle className="text-base">
            {addressTypeLabel(row.address_type)}
          </CardTitle>
          <Badge variant="outline">{row.source}</Badge>
          {row.is_canonical_source ? (
            <Badge variant="secondary">canonical source</Badge>
          ) : null}
          {row.is_foreign ? <Badge variant="outline">foreign</Badge> : null}
          {row.has_address ? null : (
            <Badge variant="outline">no address recorded</Badge>
          )}
        </div>
        <CardDescription>
          {row.display_address === ""
            ? "This source recorded no displayable address."
            : row.display_address}
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <DefinitionList
          valueClassName="break-words"
          entries={[
            ["Care of", text(row.care_of)],
            ["Street", text(row.street_address)],
            ["Postal code", text(row.postal_code)],
            ["Post town", text(row.post_town)],
            ["Country (source)", text(row.country_code)],
            ["Country (resolved)", text(row.resolved_country_code)],
            [
              "Raw address",
              <span className="break-all">{text(row.raw_address)}</span>,
            ],
            [
              "Normalized",
              <span className="break-all font-mono text-xs">
                {text(row.normalized_address)}
              </span>,
            ],
            [
              "Address key",
              <code className="font-mono text-xs">
                {row.address_key.slice(0, 12)}
              </code>,
            ],
            ["Observed", text(row.observed_at)],
            ["Updated from raw", text(row.updated_from_raw_at)],
          ]}
        />
        <div className="flex flex-col gap-1 border-t pt-3">
          <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Canonical address and geocode
          </span>
          {/* A row that address identity has not folded into a canonical
              address yet has nothing downstream to show, and a grid of em
              dashes reads as "the geocoder failed" rather than "it has not
              run". Say which one it is. */}
          {row.has_link ? (
            <DefinitionList
              valueClassName="break-words"
              entries={[
                ["Canonical address", text(row.canonical_display_address)],
                [
                  "Address id",
                  row.address_id === "" ? (
                    EMPTY_VALUE
                  ) : (
                    <code className="font-mono text-xs">
                      {row.address_id.slice(0, 12)}
                    </code>
                  ),
                ],
                ["Link review", text(row.link_review_status)],
                ["Geocode status", text(row.geocode_status)],
                ["Geocode precision", text(row.geocode_precision)],
                ["Geocode provider", text(row.geocode_provider)],
                ["Match method", text(row.geocode_match_method)],
                ["Match confidence", text(row.geocode_match_confidence)],
                [
                  "Coordinates",
                  hasPoint ? (
                    <a
                      className="underline underline-offset-2 tabular-nums"
                      href={openStreetMapHref(row.latitude, row.longitude)}
                      target="_blank"
                      rel="noreferrer"
                    >
                      {row.latitude}, {row.longitude}
                    </a>
                  ) : (
                    EMPTY_VALUE
                  ),
                ],
                ["Geocoded at", text(row.geocoded_at)],
              ]}
            />
          ) : (
            <p className="text-sm text-muted-foreground">
              Not linked to a canonical address yet, so there is nothing to
              geocode against.
            </p>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

/**
 * Every registered address of this company, one card per (type, source) --
 * the register's own grain. Two sources agreeing is a fact worth seeing, so
 * they are not merged here.
 */
export function SeCompanyAddressTab({
  addresses,
}: {
  addresses: SeCompanyAddressRow[];
}) {
  if (addresses.length === 0) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <MapPinIcon />
          </EmptyMedia>
          <EmptyTitle>No address recorded</EmptyTitle>
          <EmptyDescription>
            No source has published an address for this company yet.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  return (
    <section className="flex flex-col gap-4">
      {addresses.map((row) => (
        <AddressCard
          key={`${row.address_type}:${row.source}:${row.address_key}`}
          row={row}
        />
      ))}
    </section>
  );
}
