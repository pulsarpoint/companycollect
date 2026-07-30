/**
 * A notice's CPV classification, as a list of subjects.
 *
 * Joining the decoded subjects with " · " reproduced the problem decoding was
 * meant to solve — one long unbreakable line that a reader scans rather than
 * reads:
 *
 *   Architectural, engineering and inspection services (71314200) · Energy and
 *   fuel (09310000) · Financial and insurance services (66140000)
 *
 * A procurement covering three subjects is three facts, so it gets three rows.
 * The code sits beside its label because the division name is a summary and the
 * code is the exact thing the buyer stated; `title` carries every code the
 * notice listed for that division, which is where the ancestor chain
 * (71000000 → 71300000 → 71310000 → 71314000 → 71314200) remains checkable
 * without being printed.
 */

import { cpvSubjects, type CpvSubject } from "~/lib/cpv";

export function CpvSubjectList({ raw }: { raw: unknown }) {
  const subjects = cpvSubjects(raw);
  if (subjects.length === 0) return null;
  return <CpvSubjectRows subjects={subjects} />;
}

export function CpvSubjectRows({ subjects }: { subjects: CpvSubject[] }) {
  return (
    <ul className="flex flex-col gap-1">
      {subjects.map((subject) => (
        <li
          key={subject.division}
          className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5"
          title={subject.codes.join(", ")}
        >
          <span className="text-sm">{subject.label}</span>
          <span className="text-muted-foreground font-mono text-xs tabular-nums">
            {subject.code}
          </span>
          {subject.codes.length > 1 ? (
            // The notice listed the same subject at several depths. Said as a
            // count rather than printed, so the row stays one line.
            <span className="text-muted-foreground/60 text-[10px]">
              {subject.codes.length} codes
            </span>
          ) : null}
        </li>
      ))}
    </ul>
  );
}
