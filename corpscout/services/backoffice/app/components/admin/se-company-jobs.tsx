import { BriefcaseIcon, ExternalLinkIcon, XIcon } from "lucide-react";
import { Link, useLocation } from "react-router";
import { Badge } from "~/components/ui/badge";
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import type {
  SeCompanyJobAdContact,
  SeCompanyJobAdDetail,
  SeCompanyJobAdRequirement,
  SeCompanyJobRow,
} from "~/lib/se-company-jobs.server";

/** The date part of a ClickHouse DateTime64 string, or '' as it came. */
function day(value: string): string {
  return value.slice(0, 10);
}

/**
 * The ad text ready for whitespace-pre-line rendering: newlines normalized
 * and -- for the rare ad that arrived as HTML (1 of 21k in the live data) --
 * tags stripped to line breaks rather than injected into the page. Plain
 * text passes through untouched apart from newline form.
 */
export function normalizeAdText(text: string): string {
  let out = text.replace(/\r\n?/g, "\n");
  if (/<[a-z][^>]*>|<\/[a-z]+>/i.test(out)) {
    out = out
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/(p|div|li|ul|ol|h[1-6]|tr)>/gi, "\n")
      .replace(/<[^>]+>/g, "")
      .replace(/&nbsp;/gi, " ")
      .replace(/&amp;/gi, "&")
      .replace(/&lt;/gi, "<")
      .replace(/&gt;/gi, ">")
      .replace(/&quot;/gi, '"')
      .replace(/&#39;/g, "'");
  }
  return out.replace(/\n{3,}/g, "\n\n").trim();
}

export type AdTextBlock =
  | { kind: "heading"; text: string }
  | { kind: "paragraph"; text: string }
  | { kind: "list"; items: string[] };

const BULLET_RE = /^[●•▪◦*–-]\s+/;

/** Platsbanken ad text is plain text with strong conventions: `●` bullet
 * lines, short colon-ended lead-ins ("Vi erbjuder:") and short standalone
 * heading lines ("Arbetsbeskrivning"). This turns the normalized text into
 * renderable blocks so the ad reads like the original posting instead of a
 * wall of lines. Conservative on purpose: anything unrecognized is a
 * paragraph. */
export function formatAdText(text: string): AdTextBlock[] {
  const blocks: AdTextBlock[] = [];
  let list: string[] | null = null;
  const flushList = () => {
    if (list !== null && list.length > 0) blocks.push({ kind: "list", items: list });
    list = null;
  };
  for (const raw of normalizeAdText(text).split("\n")) {
    const line = raw.trim();
    if (line === "") {
      flushList();
      continue;
    }
    if (BULLET_RE.test(line)) {
      (list ??= []).push(line.replace(BULLET_RE, "").trim());
      continue;
    }
    flushList();
    const isHeading =
      line.length <= 60 &&
      !/[.!?…]$/.test(line) &&
      (/:$/.test(line) || (!line.includes(" http") && line.split(/\s+/).length <= 6));
    blocks.push({ kind: isHeading ? "heading" : "paragraph", text: line });
  }
  flushList();
  return blocks;
}

const URL_RE = /(https?:\/\/[^\s)]+)/g;

/** Inline text with bare URLs turned into external links. */
function AdInlineText({ text }: { text: string }) {
  const parts = text.split(URL_RE);
  return (
    <>
      {parts.map((part, index) =>
        /^https?:\/\//.test(part) ? (
          <a
            key={index}
            href={part}
            target="_blank"
            rel="noreferrer"
            className="break-all underline underline-offset-2"
          >
            {part}
          </a>
        ) : (
          <span key={index}>{part}</span>
        ),
      )}
    </>
  );
}

function AdTextBlocks({ blocks }: { blocks: AdTextBlock[] }) {
  return (
    <div className="max-w-prose space-y-2.5 text-sm leading-relaxed">
      {blocks.map((block, index) =>
        block.kind === "heading" ? (
          <p key={index} className="mt-4 font-medium first:mt-0">
            <AdInlineText text={block.text} />
          </p>
        ) : block.kind === "list" ? (
          <ul key={index} className="list-disc space-y-1 pl-5">
            {block.items.map((item, itemIndex) => (
              <li key={itemIndex}>
                <AdInlineText text={item} />
              </li>
            ))}
          </ul>
        ) : (
          <p key={index}>
            <AdInlineText text={block.text} />
          </p>
        ),
      )}
    </div>
  );
}

/** Human wording for the verified requirement_type vocabulary; an unknown
 * value falls back to its own words instead of hiding. */
const REQUIREMENT_TYPE_LABELS: Record<string, string> = {
  skill: "skill",
  language: "language",
  work_experience: "experience",
  education: "education",
  education_level: "education level",
  driving_license: "driving licence",
};

function requirementTypeLabel(type: string): string {
  return REQUIREMENT_TYPE_LABELS[type] ?? type.replaceAll("_", " ");
}

/**
 * '50–100%' for a partial scope, '75%' for a fixed partial one, and '' when
 * the scope is unknown or the default full 100/100 (working hours already
 * say 'Heltid' there -- repeating 100% is noise).
 */
export function scopeText(
  scopeMin: number | null,
  scopeMax: number | null,
): string {
  const min = scopeMin ?? scopeMax;
  const max = scopeMax ?? scopeMin;
  if (min === null || max === null) return "";
  if (min === 100 && max === 100) return "";
  return min === max ? `${min}%` : `${min}–${max}%`;
}

/** A Nullable(UInt8) flag as words; '' when the source did not say. */
function flagText(value: number | null): string {
  if (value === null) return "";
  return value === 1 ? "yes" : "no";
}

function DetailFact({ label, value }: { label: string; value: string }) {
  if (value === "") return null;
  return (
    <div className="flex gap-2 text-sm">
      <dt className="text-muted-foreground w-32 shrink-0">{label}</dt>
      <dd className="min-w-0 break-words">{value}</dd>
    </div>
  );
}

function RequirementGroup({
  title,
  requirements,
}: {
  title: string;
  requirements: SeCompanyJobAdRequirement[];
}) {
  if (requirements.length === 0) return null;
  return (
    <div className="flex flex-col gap-1">
      <span className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
        {title}
      </span>
      <ul className="flex flex-wrap gap-1.5">
        {requirements.map((requirement, index) => (
          <li
            key={`${requirement.requirement_type}:${requirement.label_original}:${index}`}
            className="flex items-center gap-1"
          >
            <Badge variant="secondary">{requirement.label_original}</Badge>
            <Badge variant="outline">
              {requirementTypeLabel(requirement.requirement_type)}
            </Badge>
          </li>
        ))}
      </ul>
    </div>
  );
}

function ContactLine({ contact }: { contact: SeCompanyJobAdContact }) {
  const name = contact.name === "" ? contact.description : contact.name;
  return (
    <li className="text-sm">
      <span>{name === "" ? "Contact" : name}</span>
      {contact.contact_type === "" ? null : (
        <span className="text-muted-foreground ml-1 text-xs">
          {contact.contact_type}
        </span>
      )}
      {contact.email === "" ? null : (
        <a
          className="ml-2 text-xs underline underline-offset-2"
          href={`mailto:${contact.email}`}
        >
          {contact.email}
        </a>
      )}
      {contact.telephone === "" ? null : (
        <span className="text-muted-foreground ml-2 text-xs tabular-nums">
          {contact.telephone}
        </span>
      )}
    </li>
  );
}

/**
 * The `?ad=` detail: one Card above the list with the latest version's
 * salary/scope/address/application facts, the structured requirements split
 * must-have vs nice-to-have, the recruiting contacts and the full ad text.
 * The close link drops the search param and keeps the tab path.
 */
function SeCompanyJobAdDetailCard({
  detail,
  closeTo,
}: {
  detail: SeCompanyJobAdDetail;
  closeTo: string;
}) {
  const extras = detail.extras;
  const mustHave = detail.requirements.filter(
    (requirement) => requirement.requirement_level === "must_have",
  );
  const niceToHave = detail.requirements.filter(
    (requirement) => requirement.requirement_level === "nice_to_have",
  );
  const otherLevel = detail.requirements.filter(
    (requirement) =>
      requirement.requirement_level !== "must_have" &&
      requirement.requirement_level !== "nice_to_have",
  );
  const address =
    extras === null
      ? ""
      : [
          extras.street_address,
          [extras.postcode, extras.city].filter((part) => part !== "").join(" "),
        ]
          .filter((part) => part !== "")
          .join(", ");
  const description = normalizeAdText(detail.description_text_original);
  return (
    <Card>
      <CardHeader>
        <div className="flex items-start justify-between gap-2">
          <CardTitle className="text-base">
            {detail.headline_original === ""
              ? detail.source_job_ad_id
              : detail.headline_original}
          </CardTitle>
          <Link
            to={closeTo}
            aria-label="Close ad detail"
            className="text-muted-foreground hover:text-foreground shrink-0"
          >
            <XIcon className="size-4" />
          </Link>
        </div>
        <CardDescription className="flex flex-wrap items-center gap-2">
          <span>Ad {detail.source_job_ad_id}</span>
          {detail.detected_language === "" ? null : (
            <Badge variant="outline">{detail.detected_language}</Badge>
          )}
          {detail.webpage_url === "" ? null : (
            <a
              className="inline-flex items-center gap-1 underline underline-offset-2"
              href={detail.webpage_url}
              target="_blank"
              rel="noreferrer"
            >
              Platsbanken <ExternalLinkIcon className="size-3" />
            </a>
          )}
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {extras === null ? (
          <p className="text-muted-foreground text-sm">
            No raw ad version found, so salary, address and application details
            are unavailable for this ad.
          </p>
        ) : (
          <dl className="grid gap-x-8 gap-y-1 sm:grid-cols-2">
            <DetailFact
              label="Salary"
              value={[extras.salary_type_label, extras.salary_description]
                .filter((part) => part !== "")
                .join(" · ")}
            />
            <DetailFact
              label="Scope"
              value={scopeText(extras.scope_min, extras.scope_max)}
            />
            <DetailFact
              label="Experience"
              value={
                extras.experience_required === null
                  ? ""
                  : extras.experience_required === 1
                    ? "required"
                    : "not required"
              }
            />
            <DetailFact
              label="Driving licence"
              value={flagText(extras.driving_license_required)}
            />
            <DetailFact
              label="Own car"
              value={flagText(extras.access_to_own_car)}
            />
            <DetailFact label="Workplace" value={extras.employer_workplace} />
            <DetailFact label="Address" value={address} />
            <DetailFact label="Apply by email" value={extras.application_email} />
            <DetailFact label="Apply at" value={extras.application_url} />
            <DetailFact
              label="Application"
              value={extras.application_information}
            />
          </dl>
        )}
        {detail.requirements.length === 0 ? null : (
          <div className="flex flex-col gap-3">
            <RequirementGroup title="Must have" requirements={mustHave} />
            <RequirementGroup title="Nice to have" requirements={niceToHave} />
            <RequirementGroup title="Other" requirements={otherLevel} />
          </div>
        )}
        {detail.contacts.length === 0 ? null : (
          <div className="flex flex-col gap-1">
            <span className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
              Contacts
            </span>
            <ul className="flex flex-col gap-1">
              {detail.contacts.map((contact) => (
                <ContactLine key={contact.contact_index} contact={contact} />
              ))}
            </ul>
          </div>
        )}
        {description === "" ? null : (
          <div className="flex flex-col gap-2 border-t pt-4">
            <span className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
              Ad text
            </span>
            <AdTextBlocks blocks={formatAdText(description)} />
          </div>
        )}
      </CardContent>
    </Card>
  );
}

/**
 * The company's job ads from corpscout.company_job_history, newest interval
 * first. Currently-open ads (still in company_job_current, or with no
 * recorded end) wear an `open` badge; an estimated end is marked rather than
 * passed off as a fact the source stated. Clicking a headline sets `?ad=` and
 * the loader answers with the ad's full detail, rendered as a Card above the
 * list.
 */
export function SeCompanyJobsTab({
  jobs,
  adDetail = null,
}: {
  jobs: SeCompanyJobRow[];
  adDetail?: SeCompanyJobAdDetail | null;
}) {
  const { pathname } = useLocation();
  if (jobs.length === 0) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <BriefcaseIcon />
          </EmptyMedia>
          <EmptyTitle>No job ads recorded for this company</EmptyTitle>
          <EmptyDescription>
            The Platsbanken pipeline holds ads for other companies, but neither
            the historical archives nor the live JobStream feed matched any to
            this one.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }
  const openCount = jobs.filter((job) => job.is_open === 1).length;
  return (
    <div className="flex flex-col gap-4">
      {adDetail === null ? null : (
        <SeCompanyJobAdDetailCard detail={adDetail} closeTo={pathname} />
      )}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Job ads</CardTitle>
          <p className="text-muted-foreground text-sm">
            {jobs.length} ad interval{jobs.length === 1 ? "" : "s"}
            {openCount > 0 ? ` · ${openCount} currently open` : ""}
          </p>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="overflow-x-auto">
            <Table className="min-w-[62rem]">
              <TableHeader>
                <TableRow>
                  <TableHead>Headline</TableHead>
                  <TableHead>Occupation</TableHead>
                  <TableHead>Location</TableHead>
                  <TableHead>Active</TableHead>
                  <TableHead>Deadline</TableHead>
                  <TableHead>Source</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {jobs.map((job) => (
                  <TableRow
                    key={`${job.source_system}:${job.source_job_ad_id}:${job.interval_number}`}
                    data-state={
                      adDetail?.source_job_ad_id === job.source_job_ad_id
                        ? "selected"
                        : undefined
                    }
                  >
                    <TableCell className="align-top">
                      <div className="flex flex-wrap items-center gap-2">
                        <Link
                          className="underline-offset-2 hover:underline"
                          to={`?ad=${encodeURIComponent(job.source_job_ad_id)}`}
                        >
                          {job.headline_original === ""
                            ? job.source_job_ad_id
                            : job.headline_original}
                        </Link>
                        {job.is_open === 1 ? <Badge>open</Badge> : null}
                        {job.number_of_vacancies > 1 ? (
                          <Badge variant="outline">
                            {job.number_of_vacancies} vacancies
                          </Badge>
                        ) : null}
                        {job.webpage_url === "" ? null : (
                          <a
                            className="text-muted-foreground hover:text-foreground"
                            href={job.webpage_url}
                            target="_blank"
                            rel="noreferrer"
                            aria-label="Open ad on Platsbanken"
                          >
                            <ExternalLinkIcon className="size-3.5" />
                          </a>
                        )}
                      </div>
                    </TableCell>
                    <TableCell className="align-top">
                      {job.occupation_label === "" ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        job.occupation_label
                      )}
                      {job.employment_type_label !== "" ||
                      job.working_hours_label !== "" ? (
                        <div className="text-muted-foreground text-xs">
                          {[job.employment_type_label, job.working_hours_label]
                            .filter((part) => part !== "")
                            .join(" · ")}
                        </div>
                      ) : null}
                    </TableCell>
                    <TableCell
                      className="align-top"
                      title={job.region_name === "" ? undefined : job.region_name}
                    >
                      {job.municipality_name === "" ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        job.municipality_name
                      )}
                    </TableCell>
                    <TableCell className="align-top tabular-nums whitespace-nowrap">
                      {day(job.active_from)}
                      {" – "}
                      {job.active_to === "" ? (
                        <span className="text-muted-foreground">open-ended</span>
                      ) : (
                        <>
                          {day(job.active_to)}
                          {job.is_end_estimated === 1 ? (
                            <span
                              className="text-muted-foreground"
                              title={
                                job.active_to_basis === ""
                                  ? "estimated end"
                                  : `estimated end (${job.active_to_basis})`
                              }
                            >
                              {" "}
                              (est.)
                            </span>
                          ) : null}
                        </>
                      )}
                    </TableCell>
                    <TableCell className="align-top tabular-nums whitespace-nowrap">
                      {job.application_deadline === "" ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        day(job.application_deadline)
                      )}
                    </TableCell>
                    <TableCell className="align-top">
                      <Badge variant="secondary">{job.source_system}</Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
          <p className="text-muted-foreground text-xs">
            One row per active interval of an ad: an ad taken down and
            republished appears once per interval. Ends marked (est.) are
            estimated by the pipeline, not stated by the source.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
