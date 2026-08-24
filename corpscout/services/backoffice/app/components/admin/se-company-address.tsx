import { CheckCircle2Icon, MapPinIcon, TriangleAlertIcon } from "lucide-react";
import { Form, useNavigation } from "react-router";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Checkbox } from "~/components/ui/checkbox";
import { CompanySourceStrip } from "~/components/admin/company-source-strip";
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
import { Input } from "~/components/ui/input";
import {
  correctionStatus,
  OVERRIDABLE_FIELDS,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-address-corrections";
import {
  ADDRESS_FIELD_LABELS,
  liveOverrideRefusal,
} from "~/lib/se-address-review-form";
import type {
  SeCompanyAddressCorrectionRow,
  SeCompanyAddressDetail,
  SeCompanyAddressRow,
} from "~/lib/se-company-address.server";

/**
 * Every published address of one company, and the reviewer's decisions on them.
 *
 * Only LIVE rows carry decision forms. The removed section offers undo alone: an
 * override written against a row a reject has tombstoned is the stale trap this
 * page must not create -- Dagster drops it on the next run without telling
 * anyone. (A reject written outside this page with no address_key in its payload
 * has no subject at all: address_rules.py skips it silently, so the ledger shows
 * it pending for ever. Nothing here can fix that; the ledger list is where it
 * would be spotted.)
 */

export type SeCompanyAddressReviewResult =
  | { ok: true; correctionId: string }
  | { ok: false; error: string }
  | null;

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

/**
 * What every non-undo decision posts: the kind, the address_key it decides, and
 * the evidence hash the reviewer actually SAW. The append re-reads the named row
 * and refuses a decision whose hash has moved, so a page left open while Dagster
 * republished cannot silently decide different evidence.
 */
function HiddenDecision({
  kind,
  addressKey,
  evidenceHash,
}: {
  kind: string;
  addressKey: string;
  evidenceHash: string;
}) {
  return (
    <>
      <input type="hidden" name="correction_kind" value={kind} />
      <input type="hidden" name="address_key" value={addressKey} />
      <input type="hidden" name="evidence_hash" value={evidenceHash} />
    </>
  );
}

/**
 * The override form for one row: one input per overridable field, each with the
 * text it was rendered with (diffed server-side, so an untouched field never
 * enters the payload) and its own clear box (an emptied input is not a decision;
 * clearing a field is an explicit null).
 *
 * address_type is not here on purpose: it is part of address_key, so changing it
 * would move the row to a different identity -- reject the address instead.
 */
function OverrideForm({
  row,
  refusal,
  busy,
}: {
  row: SeCompanyAddressRow;
  refusal: string | null;
  busy: boolean;
}) {
  const closed = refusal !== null;
  return (
    <div className="flex flex-col gap-2 border-t pt-3">
      <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        Override fields
      </span>
      <Form method="post" className="flex flex-col gap-3">
        <HiddenDecision
          kind="override_field"
          addressKey={row.address_key}
          evidenceHash={row.evidence_set_hash}
        />
        <div className="grid gap-3 md:grid-cols-2">
          {OVERRIDABLE_FIELDS.map((field) => (
            <div key={field} className="flex flex-col gap-1">
              <input type="hidden" name={`original_${field}`} value={row[field]} />
              <Input
                name={field}
                defaultValue={row[field]}
                aria-label={ADDRESS_FIELD_LABELS[field]}
                placeholder={ADDRESS_FIELD_LABELS[field]}
                disabled={closed}
              />
              <label className="flex items-center gap-2 text-xs text-muted-foreground">
                <Checkbox name={`clear_${field}`} value="yes" disabled={closed} />
                <span>Clear {ADDRESS_FIELD_LABELS[field].toLowerCase()}</span>
              </label>
            </div>
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Input
            name="reason"
            placeholder="Reason"
            aria-label="Reason"
            required
            className="w-64"
            disabled={closed}
          />
          <Button type="submit" size="sm" disabled={busy || closed} aria-busy={busy}>
            Save override
          </Button>
          {refusal ? (
            <span className="text-xs text-muted-foreground">{refusal}</span>
          ) : null}
        </div>
      </Form>
    </div>
  );
}

/** "This is not an address of this company": Dagster republishes the row
 * is_current = false. It decides nothing but the key -- a reject carrying any
 * other payload field is skipped as malformed. */
function RejectForm({ row, busy }: { row: SeCompanyAddressRow; busy: boolean }) {
  return (
    <Form method="post" className="flex flex-wrap items-center gap-2 border-t pt-3">
      <HiddenDecision
        kind="reject_address"
        addressKey={row.address_key}
        evidenceHash={row.evidence_set_hash}
      />
      <Input
        name="reason"
        placeholder="Why reject"
        aria-label="Why reject"
        required
        className="w-64"
      />
      <Button size="sm" variant="outline" type="submit" disabled={busy} aria-busy={busy}>
        Reject address
      </Button>
    </Form>
  );
}

/**
 * This row's ledger, newest first, with an undo on every live decision --
 * including on a removed card, which is the only way a rejected address comes
 * back.
 *
 * An undo cannot itself be undone: it supersedes a correction, and superseding
 * the superseder would leave the pipeline reading a chain nothing in
 * effective_ledger walks.
 */
function CorrectionList({
  corrections,
  busy,
}: {
  corrections: SeCompanyAddressCorrectionRow[];
  busy: boolean;
}) {
  if (corrections.length === 0) return null;
  return (
    <ul className="flex flex-col gap-2 border-t pt-3 text-sm">
      {corrections.map((correction) => (
        <li key={correction.correction_id} className="flex flex-col gap-1">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline">{correction.correction_kind}</Badge>
            <Badge variant="secondary">{correctionStatus(correction)}</Badge>
            <code className="font-mono text-xs text-muted-foreground">
              {correction.correction_id.slice(0, 8)}
            </code>
            <span className="text-muted-foreground text-xs">
              {correction.decided_by} · {correction.created_at}
            </span>
            {correction.is_current === 1 && correction.correction_kind !== "undo" ? (
              <Form method="post" className="ml-auto flex items-center gap-2">
                <input type="hidden" name="correction_kind" value="undo" />
                {/* Undo supersedes a decision, not evidence. */}
                <input type="hidden" name="evidence_hash" value={ZERO_EVIDENCE_HASH} />
                <input
                  type="hidden"
                  name="supersedes_correction_id"
                  value={correction.correction_id}
                />
                <Input
                  name="reason"
                  placeholder="Why undo"
                  aria-label="Why undo"
                  required
                  className="w-40"
                />
                <Button
                  size="sm"
                  variant="ghost"
                  type="submit"
                  disabled={busy}
                  aria-busy={busy}
                >
                  Undo
                </Button>
              </Form>
            ) : null}
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
  overrideRefusal,
  decidable,
  busy,
}: {
  row: SeCompanyAddressRow;
  corrections: SeCompanyAddressCorrectionRow[];
  /** Non-null when a live override already decides this row; the form closes
   * and says so instead of writing a second one nobody would ever see. */
  overrideRefusal: string | null;
  /** Live rows can be overridden and rejected. A tombstoned one can only be
   * undone -- see the module comment. */
  decidable: boolean;
  busy: boolean;
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
        {decidable ? (
          <>
            <OverrideForm row={row} refusal={overrideRefusal} busy={busy} />
            <RejectForm row={row} busy={busy} />
          </>
        ) : null}
        <CorrectionList corrections={corrections} busy={busy} />
      </CardContent>
    </Card>
  );
}

/** Shown when no source has published an address for this company and no
 * reviewer has decided anything about one. */
export function SeCompanyAddressEmpty() {
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
  result,
}: {
  detail: SeCompanyAddressDetail;
  result: SeCompanyAddressReviewResult;
}) {
  const { addresses, removed, corrections } = detail;
  // One click is one ledger row: block every submit while one is in flight.
  const busy = useNavigation().state !== "idle";
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
    return <SeCompanyAddressEmpty />;
  }
  return (
    <div className="flex flex-col gap-6">
      {/* Every source that carried any address this tab shows, live or
          tombstoned: a rejected address is still evidence a register held it,
          and dropping it would make the strip disagree with the cards below. */}
      <CompanySourceStrip
        sources={[...addresses, ...removed].flatMap((row) => row.sources)}
      />
      {result?.ok ? (
        <Alert>
          <CheckCircle2Icon />
          <AlertTitle>Saved</AlertTitle>
          <AlertDescription>
            Correction {result.correctionId} is in the ledger. Queued for the
            next Dagster address run; reload to see the result once it lands.
          </AlertDescription>
        </Alert>
      ) : null}
      {result && !result.ok ? (
        <Alert variant="destructive">
          <TriangleAlertIcon />
          <AlertTitle>Not saved</AlertTitle>
          <AlertDescription>{result.error}</AlertDescription>
        </Alert>
      ) : null}
      <section className="flex flex-col gap-4">
        {addresses.map((row) => (
          <AddressCard
            key={row.address_key}
            row={row}
            corrections={forKey(row.address_key)}
            // The whole ledger, not this card's slice: liveOverrideCorrectionId
            // picks the row's own overrides out of it, and needs every undo to
            // know which of them still stands.
            overrideRefusal={liveOverrideRefusal(
              "override_field",
              row.address_key,
              corrections,
            )}
            decidable
            busy={busy}
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
              visible and can be undone — undo is the only decision offered
              here, since anything else would be dropped on the next run.
            </p>
          </div>
          {removed.map((row) => (
            <AddressCard
              key={row.address_key}
              row={row}
              corrections={forKey(row.address_key)}
              overrideRefusal={null}
              decidable={false}
              busy={busy}
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
          <CorrectionList corrections={orphaned} busy={busy} />
        </section>
      )}
    </div>
  );
}
