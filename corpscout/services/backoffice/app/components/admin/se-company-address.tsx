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
import { correctionStatus } from "~/lib/se-address-corrections";
import type {
  SeCompanyAddressCorrectionRow,
  SeCompanyAddressDetail,
  SeCompanyAddressRow,
} from "~/lib/se-company-address.server";

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

/** One published address as a line a reader recognises, from the parts the
 * final stores. */
function displayAddress(row: SeCompanyAddressRow): string {
  const locality = [row.postal_code, row.city].filter((part) => part !== "").join(" ");
  return [row.care_of, row.street_address, locality]
    .filter((part) => part !== "")
    .join(", ");
}

function CorrectionList({
  corrections,
}: {
  corrections: SeCompanyAddressCorrectionRow[];
}) {
  if (corrections.length === 0) return null;
  return (
    <ul className="flex flex-col gap-2 border-t pt-3 text-sm">
      {corrections.map((correction) => (
        <li key={correction.correction_id} className="flex flex-col gap-1">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline">{correction.correction_kind}</Badge>
            <Badge variant="secondary">{correctionStatus(correction)}</Badge>
            <span className="text-muted-foreground text-xs">
              {correction.decided_by} · {correction.created_at}
            </span>
          </div>
          <p className="text-muted-foreground break-words">{correction.reason}</p>
        </li>
      ))}
    </ul>
  );
}

function AddressCard({
  row,
  corrections,
}: {
  row: SeCompanyAddressRow;
  corrections: SeCompanyAddressCorrectionRow[];
}) {
  const hasPoint = row.latitude !== "" && row.longitude !== "";
  const line = displayAddress(row);
  // A correction whose evidence no longer matches this row was dropped by
  // Dagster: say so on the card, not only in the ledger list.
  const hasStale = corrections.some((correction) => correction.is_stale === 1);
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center gap-2">
          <CardTitle className="text-base">
            {addressTypeLabel(row.address_type)}
          </CardTitle>
          {/* In the row's own order: precedence between sources is visible. */}
          {row.sources.map((source) => (
            <Badge key={source} variant="outline">
              {source}
            </Badge>
          ))}
          {hasStale ? <Badge variant="secondary">evidence changed</Badge> : null}
        </div>
        <CardDescription>
          {line === "" ? "This address has no displayable text." : line}
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <DefinitionList
          valueClassName="break-words"
          entries={[
            ["Care of", text(row.care_of)],
            ["Street", text(row.street_address)],
            ["Postal code", text(row.postal_code)],
            ["City", text(row.city)],
            ["Country", text(row.country_code)],
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
          ]}
        />
        <div className="flex flex-col gap-1 border-t pt-3">
          <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Geocode
          </span>
          {/* An address that never reached the geocoder has nothing to show,
              and a grid of em dashes reads as "the geocoder failed" rather
              than "it never ran". Say which one it is. */}
          {row.geocode_status === "" ? (
            <p className="text-sm text-muted-foreground">
              This address has not been geocoded.
            </p>
          ) : (
            <DefinitionList
              valueClassName="break-words"
              entries={[
                ["Geocode status", text(row.geocode_status)],
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
              ]}
            />
          )}
        </div>
        <details className="border-t pt-3 text-sm">
          <summary className="cursor-pointer text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Provenance
          </summary>
          <DefinitionList
            className="mt-2"
            valueClassName="break-all"
            entries={[
              [
                "Source records",
                row.source_record_uids.length === 0
                  ? EMPTY_VALUE
                  : row.source_record_uids.join(", "),
              ],
              ["Resolved at", text(row.resolved_at)],
              [
                "Evidence set",
                <code className="font-mono text-xs">
                  {row.evidence_set_hash.slice(0, 12)}
                </code>,
              ],
            ]}
          />
        </details>
        <CorrectionList corrections={corrections} />
      </CardContent>
    </Card>
  );
}

/**
 * Every published address of this company, one card per address_key -- the
 * datatype's own grain. Two sources agreeing on an address share one row and
 * show both badges.
 *
 * Tombstoned rows get their own section (ruling A8): a rejected address that
 * simply vanished would take its correction with it, and the undo that brings
 * it back would be unreachable.
 */
export function SeCompanyAddressTab({
  detail,
}: {
  detail: SeCompanyAddressDetail;
}) {
  const { addresses, removed, corrections } = detail;
  // An undo names a correction, not an address, so it is grouped under the
  // address of the correction it supersedes -- otherwise every undo would fall
  // out of the card whose history it belongs to.
  const keyById = new Map(
    corrections.map((correction) => [correction.correction_id, correction.address_key]),
  );
  const keyOf = (correction: SeCompanyAddressCorrectionRow): string =>
    correction.address_key !== ""
      ? correction.address_key
      : keyById.get(correction.supersedes_correction_id ?? "") ?? "";
  const forKey = (key: string) =>
    corrections.filter((correction) => keyOf(correction) === key);
  const carded = new Set([...addresses, ...removed].map((row) => row.address_key));
  const orphaned = corrections.filter((correction) => !carded.has(keyOf(correction)));

  if (addresses.length === 0 && removed.length === 0 && corrections.length === 0) {
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
    <div className="flex flex-col gap-6">
      <section className="flex flex-col gap-4">
        {addresses.map((row) => (
          <AddressCard
            key={row.address_key}
            row={row}
            corrections={forKey(row.address_key)}
          />
        ))}
      </section>
      {removed.length === 0 ? null : (
        <section className="flex flex-col gap-4">
          <div className="flex flex-col gap-1">
            <h2 className="text-sm font-medium uppercase tracking-wide text-muted-foreground">
              Removed / rejected
            </h2>
            <p className="text-sm text-muted-foreground">
              Addresses this company no longer has: rejected by a reviewer, or
              no longer carried by any source. Kept so the decision stays
              visible and can be undone.
            </p>
          </div>
          {removed.map((row) => (
            <AddressCard
              key={row.address_key}
              row={row}
              corrections={forKey(row.address_key)}
            />
          ))}
        </section>
      )}
      {orphaned.length === 0 ? null : (
        <section className="flex flex-col gap-2">
          <h2 className="text-sm font-medium uppercase tracking-wide text-muted-foreground">
            Corrections without an address
          </h2>
          {/* A reject naming a key this company never published (or has long
              since dropped) is applied the moment it is written -- Dagster has
              no row to stamp it on. It has no card to sit under, and hiding it
              would leave a decision nobody can see or undo. */}
          <CorrectionList corrections={orphaned} />
        </section>
      )}
    </div>
  );
}
