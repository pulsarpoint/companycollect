/**
 * Doffin (Norway) notice presentation.
 *
 * Doffin publishes eForms, like TED, but it is NOT the same record and does not
 * share TED's view. Doffin's search API already resolves winners per lot while
 * publishing no realized value, so the money comes from the notice XML and the
 * parties from the JSON — and the register carries fields TED has no equivalent
 * for (`contract_folder_id`, the published `RegulatoryDomain` flag, a per-notice
 * award result). Rendered generically it printed `value_amount_usd` as a label
 * and `19651230705` as a value.
 *
 * The "original" beside each label is the eForms BUSINESS TERM, not the column
 * name, because that is what a reader checks a figure against: BT-720 is the
 * amount payable to one winner, BT-161 the realized total for the whole notice,
 * BT-27 an estimate made before the competition.
 *
 * Those three are separate rows and are never merged. They answer different
 * questions, and on 440 of the 15,497 rows that publish both, BT-720 exceeds
 * BT-161 — an inconsistency in the register that a single "contract value"
 * column would silently pick a side on.
 */

import { CpvSubjectList } from "~/components/detail/cpv-list";
import { eformsMoney, noticeText } from "~/lib/notice-money";

export type DoffinField = { key: string; en: string; term?: string };

const AWARD_RESULTS: Record<string, string> = {
  winner_selected: "A winner was named",
  no_winner_named: "No winner was named",
};

/** What the award_result column means, or the stored value when unmapped. */
export function doffinAwardResult(value: unknown): string | null {
  const text = noticeText(value);
  if (text === null) return null;
  return AWARD_RESULTS[text] ?? text;
}

/**
 * The published cbc:RegulatoryDomain flag.
 *
 * Published, not inferred: the notice itself names the directive it was issued
 * under ('32014L0024' is Directive 2014/24/EU) or says 'other' for a nationally
 * regulated procurement. The ingest reduces that to yes/no.
 */
export function doffinDirective(value: unknown): string | null {
  const text = noticeText(value);
  if (text === null) return null;
  if (text === "yes") return "Governed by an EU procurement directive";
  if (text === "no") return "Nationally regulated";
  return text;
}

/**
 * A note when this winner's award exceeds the whole notice it belongs to.
 *
 * Both figures are stored and shown exactly as Doffin published them — that is
 * the rule, and correcting a register's arithmetic here would make the page
 * disagree with the source document it links to. But a reader seeing 200bn NOK
 * beside 2bn NOK deserves to be told the register is what disagrees, not the
 * conversion. Bærum kommune's 2025-120612 is the extreme case: 200,000,000,000
 * NOK for one electricity contract, 100× its own notice total and roughly an
 * eighth of Norway's GDP. Verified against the source XML — Doffin really does
 * publish it.
 */
export function doffinValueInconsistency(
  fields: Record<string, unknown>,
): string | null {
  const winner = Number(noticeText(fields.value_amount_original));
  const notice = Number(noticeText(fields.notice_value_amount_original));
  if (!Number.isFinite(winner) || !Number.isFinite(notice)) return null;
  if (winner <= 0 || notice <= 0) return null;

  const ratio = winner / notice;
  // A hair over is rounding between two independently published figures, not a
  // contradiction worth interrupting the reader for.
  if (ratio < 1.01) return null;

  const times = ratio >= 10 ? `${Math.round(ratio)}×` : `${ratio.toFixed(1)}×`;
  return (
    `Doffin publishes a larger amount for this winner (BT-720) than for the ` +
    `whole notice (BT-161) — ${times}. Both are shown as published; the ` +
    `inconsistency is in the register, not in the conversion.`
  );
}

const CODED: Record<string, (value: unknown) => string | null> = {
  award_result: doffinAwardResult,
  directive_governed: doffinDirective,
};

/** One field's display text, or null when the register published nothing. */
export function doffinFieldValue(key: string, value: unknown): string | null {
  const coded = CODED[key];
  if (coded) return coded(value);
  if (Array.isArray(value)) {
    return value.length > 0 ? value.map((v) => String(v)).join(", ") : null;
  }
  return noticeText(value);
}

/**
 * Whether a field repeats what another field on the same record already said.
 *
 * `winner_org_number_raw` exists because a foreign winner's id never reduces to
 * nine Norwegian digits, so the raw form is the only record of what the register
 * published. For a Norwegian winner it normalises to itself, and printing both
 * shows the same number twice under two labels.
 */
export function doffinIsRedundant(
  key: string,
  fields: Record<string, unknown>,
): boolean {
  if (key !== "winner_org_number_raw") return false;
  return (
    noticeText(fields.winner_org_number_raw) === noticeText(fields.winner_org_number)
  );
}

export const DOFFIN_SECTIONS: { title: string; fields: DoffinField[] }[] = [
  {
    title: "Notice",
    fields: [
      { key: "notice_title", en: "Title", term: "BT-21" },
      { key: "notice_description", en: "Description", term: "BT-24" },
      { key: "doffin_id", en: "Doffin reference" },
      // Shared by a competition notice and its award notice, which is what
      // makes the two halves of one procurement joinable.
      { key: "contract_folder_id", en: "Procurement reference", term: "BT-04" },
      { key: "notice_type", en: "Notice type" },
      { key: "notice_status", en: "Notice status" },
      { key: "publication_date", en: "Published" },
      { key: "issue_date", en: "Issued" },
      { key: "deadline_date", en: "Tender deadline", term: "BT-131" },
      { key: "location_ids", en: "Place of performance", term: "BT-5071" },
      { key: "directive_governed", en: "Regulatory basis", term: "RegulatoryDomain" },
    ],
  },
  {
    title: "Buyer",
    fields: [
      { key: "buyer_name", en: "Buyer", term: "BT-500" },
      { key: "buyer_org_number", en: "Organisasjonsnummer", term: "BT-501" },
    ],
  },
  {
    title: "This lot and winner",
    fields: [
      { key: "lot_id", en: "Lot" },
      { key: "lot_heading", en: "Lot title" },
      { key: "winner_name", en: "Winner", term: "BT-1311" },
      { key: "winner_org_number", en: "Winner organisasjonsnummer" },
      // Kept beside the normalised form on purpose: a foreign winner's id never
      // normalises to nine digits, and the raw value is the only record of what
      // the register actually said.
      { key: "winner_org_number_raw", en: "Winner id as published" },
      { key: "winner_country", en: "Winner country" },
      { key: "received_tenders", en: "Tenders received", term: "BT-759" },
      { key: "award_result", en: "Award result" },
    ],
  },
];

/** The three monetary claims Doffin publishes, realized first. */
const DOFFIN_VALUE_FIELDS: DoffinField[] = [
  { key: "value", en: "Awarded to this winner", term: "BT-720" },
  { key: "notice_value", en: "Realized total for the notice", term: "BT-161" },
  { key: "estimated_value", en: "Estimated before the competition", term: "BT-27" },
];

function Row({ field, children }: { field: DoffinField; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-0.5 overflow-hidden">
      <dt className="text-xs leading-tight">
        <span className="text-foreground">{field.en}</span>
        {field.term ? (
          <span className="text-muted-foreground/70 ml-1.5 font-mono text-[10px]">
            {field.term}
          </span>
        ) : null}
      </dt>
      <dd className="text-sm break-words">{children}</dd>
    </div>
  );
}

export function DoffinNoticeRecord({ fields }: { fields: Record<string, unknown> }) {
  const sections = DOFFIN_SECTIONS.map((section) => ({
    title: section.title,
    rows: section.fields
      .filter((field) => !doffinIsRedundant(field.key, fields))
      .map((field) => ({ field, value: doffinFieldValue(field.key, fields[field.key]) }))
      .filter((row): row is { field: DoffinField; value: string } => row.value !== null),
  })).filter((section) => section.rows.length > 0);

  const money = DOFFIN_VALUE_FIELDS.map((field) => ({
    field,
    value: eformsMoney(fields, field.key),
  })).filter(
    (row): row is { field: DoffinField; value: { original: string; usd: string | null } } =>
      row.value !== null,
  );

  const inconsistency = doffinValueInconsistency(fields);
  const fxRate = noticeText(fields.fx_rate_to_usd);
  const fxDate = noticeText(fields.fx_rate_date);
  const hasCpv = Array.isArray(fields.cpv_codes) && fields.cpv_codes.length > 0;

  return (
    <div className="flex flex-col gap-6">
      {sections.map((section) => (
        <section key={section.title} className="flex flex-col gap-2">
          <h3 className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
            {section.title}
          </h3>
          <dl className="grid grid-cols-1 gap-x-6 gap-y-3 sm:grid-cols-2">
            {section.rows.map((row) => (
              <Row key={row.field.key} field={row.field}>
                {row.value}
              </Row>
            ))}
          </dl>
        </section>
      ))}

      {hasCpv ? (
        <section className="flex flex-col gap-2">
          <h3 className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
            What was procured
          </h3>
          <CpvSubjectList raw={fields.cpv_codes} />
        </section>
      ) : null}

      <section className="flex flex-col gap-2">
        <h3 className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
          Value
        </h3>
        {money.length === 0 ? (
          <p className="text-muted-foreground text-sm">
            This notice publishes no monetary figure.
          </p>
        ) : (
          <>
            <dl className="grid grid-cols-1 gap-x-6 gap-y-3 sm:grid-cols-2">
              {money.map(({ field, value }) => (
                <Row key={field.key} field={field}>
                  <span className="tabular-nums">{value.original}</span>
                  {value.usd ? (
                    <span className="text-muted-foreground ml-2 text-xs tabular-nums">
                      {value.usd}
                    </span>
                  ) : null}
                </Row>
              ))}
            </dl>
            {inconsistency ? (
              <p className="text-amber-700 text-xs dark:text-amber-500">{inconsistency}</p>
            ) : null}
            {fxRate && fxDate ? (
              <p className="text-muted-foreground text-xs">
                USD converted at {fxRate} on {fxDate}
                {noticeText(fields.fx_source) ? ` (${noticeText(fields.fx_source)})` : ""}.
                Each figure keeps the currency the notice stated.
              </p>
            ) : null}
          </>
        )}
      </section>
    </div>
  );
}
