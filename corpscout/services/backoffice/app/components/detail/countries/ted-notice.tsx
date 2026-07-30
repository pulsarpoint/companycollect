/**
 * TED (eForms) notice presentation, shared by every TED-covered country.
 *
 * Not per country: TED is one register serving se, fi, no and ee, and its notices
 * have the same shape in all of them. This is the exception that proves the
 * per-register rule — the axis is the REGISTER, and it happens to be per-country
 * only because most registers are national.
 *
 * The "original" beside each label is the eForms BUSINESS TERM rather than the
 * column name. That is what a reader checks a figure against: BT-720 is the
 * awarded amount per winner, BT-27 an estimate of the whole procedure, BT-709 a
 * framework ceiling. The ted_procurement parser already reasons in these terms and
 * `value_source_field` already carries them for exactly this purpose.
 *
 * Why the money is five separate rows and never one: an estimate, a framework
 * ceiling and a realized award are different claims. Measured over 100 live
 * notices, the XML carries ten distinct amounts, and the one long treated as *the*
 * contract value (BT-720) appeared on 21 of them while BT-27's estimate appeared
 * on 57. A column holding whichever was present makes all of them unreadable, and
 * a ceiling summed as spend overstates it wildly.
 */

import { CpvSubjectRows } from "~/components/detail/cpv-list";
import { cpvSubjects } from "~/lib/cpv";
import { eformsMoney, noticeText as text } from "~/lib/notice-money";

export type TedField = { key: string; en: string; term: string };

const TED_SECTIONS: { title: string; fields: TedField[] }[] = [
  {
    title: "Notice",
    fields: [
      { key: "notice_title", en: "Title", term: "BT-21 Title" },
      { key: "publication_number", en: "Publication number", term: "TED reference" },
      { key: "publication_date", en: "Published", term: "BT-05 Notice dispatch" },
      { key: "notice_type", en: "Notice type", term: "eForms notice subtype" },
      { key: "place_country", en: "Place of performance", term: "BT-5071 Place" },
    ],
  },
  {
    title: "Buyer",
    fields: [
      { key: "buyer_name", en: "Buyer", term: "BT-500 Organisation name" },
      { key: "buyer_national_id", en: "Buyer national id", term: "BT-501 Identifier" },
      { key: "buyer_country", en: "Buyer country", term: "BT-514 Country" },
    ],
  },
];

/**
 * The five monetary claims TED publishes at notice grain, each with the business
 * term behind it. Order is realized-first, because that is the figure a reader
 * usually wants and the ceilings are the ones most easily mistaken for spend.
 */
const TED_VALUE_FIELDS: TedField[] = [
  { key: "total_value", en: "Notice value, realized", term: "BT-161" },
  { key: "estimated_value", en: "Estimated value of the procedure", term: "BT-27" },
  { key: "framework_maximum", en: "Framework maximum", term: "BT-709" },
  {
    key: "framework_total_maximum",
    en: "Maximum of all framework contracts",
    term: "BT-118",
  },
  {
    key: "framework_total_approximate",
    en: "Approximate total of all framework contracts",
    term: "BT-1118",
  },
];

function Row({ field, children }: { field: TedField; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-0.5 overflow-hidden">
      <dt className="text-xs leading-tight">
        <span className="text-foreground">{field.en}</span>
        <span className="text-muted-foreground/70 ml-1.5 font-mono text-[10px]">
          {field.term}
        </span>
      </dt>
      <dd className="text-sm break-words">{children}</dd>
    </div>
  );
}

export function TedNoticeRecord({ fields }: { fields: Record<string, unknown> }) {
  const sections = TED_SECTIONS.map((section) => ({
    title: section.title,
    rows: section.fields
      .map((field) => {
        const raw = fields[field.key];
        const value = Array.isArray(raw)
          ? raw.length > 0
            ? raw.join(", ")
            : null
          : text(raw);
        return { field, value };
      })
      .filter((row): row is { field: TedField; value: string } => row.value !== null),
  })).filter((section) => section.rows.length > 0);

  const money = TED_VALUE_FIELDS.map((field) => ({
    field,
    value: eformsMoney(fields, field.key),
  })).filter(
    (row): row is { field: TedField; value: { original: string; usd: string | null } } =>
      row.value !== null,
  );

  // BT-262 is what the procurement mainly is; BT-263 what else it touches. Both
  // decoded, because a bare 8-digit code tells a reader nothing.
  const mainSubject = cpvSubjects(fields.cpv_code)[0] ?? null;
  const additional = cpvSubjects(fields.cpv_additional_codes);

  const fxRate = text(fields.fx_rate_to_usd);
  const fxDate = text(fields.fx_rate_date);

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

      {mainSubject || additional.length > 0 ? (
        <section className="flex flex-col gap-2">
          <h3 className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
            What was procured
          </h3>
          <dl className="grid grid-cols-1 gap-x-6 gap-y-3 sm:grid-cols-2">
            {mainSubject ? (
              <Row field={{ key: "cpv_code", en: "Mainly", term: "BT-262" }}>
                {mainSubject.label}
                <span className="text-muted-foreground ml-2 font-mono text-xs">
                  {mainSubject.code}
                </span>
              </Row>
            ) : null}
            {additional.length > 0 ? (
              // A list, not a " · " join: three subjects are three facts, and
              // joining them rebuilds the unreadable single line that decoding
              // the codes was meant to remove.
              <Row field={{ key: "cpv_additional", en: "Also covers", term: "BT-263" }}>
                <CpvSubjectRows subjects={additional} />
              </Row>
            ) : null}
          </dl>
        </section>
      ) : null}

      <section className="flex flex-col gap-2">
        <h3 className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
          Value
        </h3>
        {money.length === 0 ? (
          // Said rather than left blank. A notice exists because the procurement
          // was directive-governed, not because anyone stated what was paid: TED
          // publishes a realized award on 22.6% of Swedish rows, 36% of Finnish.
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
            {fxRate && fxDate ? (
              <p className="text-muted-foreground text-xs">
                USD converted at {fxRate} on {fxDate}
                {text(fields.fx_source) ? ` (${text(fields.fx_source)})` : ""}. Each
                figure keeps the currency the notice stated.
              </p>
            ) : null}
          </>
        )}
      </section>
    </div>
  );
}
