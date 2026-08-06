import type { EvidenceRef } from "~/lib/queries.server";
import { Badge } from "~/components/ui/badge";

export function evidenceSourceLabel(evidence: EvidenceRef): string {
  return (
    evidence.origins[0]?.sourceSlug.replaceAll("_", " ") ||
    evidence.recordKind.replaceAll("_", " ")
  );
}

export function EvidenceBadges({ evidence }: { evidence: EvidenceRef[] }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {evidence.map((reference) => (
        <Badge key={reference.sourceRecordUid} variant="outline">
          {evidenceSourceLabel(reference)}
          {reference.sourceDate ? ` · ${reference.sourceDate}` : ""}
        </Badge>
      ))}
    </div>
  );
}

export function EvidencePanel({ evidence }: { evidence: EvidenceRef[] }) {
  if (evidence.length === 0) return null;
  return (
    <details>
      <summary className="text-muted-foreground cursor-pointer text-xs font-medium">
        Evidence ({evidence.length})
      </summary>
      <div className="flex flex-col gap-3 pt-2">
        {evidence.map((reference) => (
          <div key={reference.sourceRecordUid} className="flex flex-col gap-1 text-xs">
            <EvidenceBadges evidence={[reference]} />
            <div className="text-muted-foreground font-mono break-all">
              {reference.recordKind} · {reference.sourceRecordUid}
            </div>
            {reference.extractionMethod ? (
              <div>
                Method: {reference.extractionMethod}
                {reference.confidence !== undefined
                  ? ` · ${Math.round(reference.confidence * 100)}% confidence`
                  : ""}
              </div>
            ) : null}
            {reference.modelName ? (
              <div>
                Model: {[reference.modelProvider, reference.modelName]
                  .filter(Boolean)
                  .join(" / ")}
                {reference.promptVersion ? ` · ${reference.promptVersion}` : ""}
              </div>
            ) : null}
            {reference.evidenceIds && reference.evidenceIds.length > 0 ? (
              <div>Evidence IDs: {reference.evidenceIds.join(", ")}</div>
            ) : null}
            {reference.evidenceLocator ? (
              <div className="text-muted-foreground break-all">
                Locator: {reference.evidenceLocator}
              </div>
            ) : null}
            {reference.origins.map((origin) => (
              <div
                key={`${origin.sourceSlug}:${origin.sourceRecordKey}:${origin.sourceObjectKey}`}
                className="flex flex-col gap-0.5"
              >
                <div>
                  {origin.sourceSlug.replaceAll("_", " ")} · {origin.sourceRecordKey}
                  {origin.retrievedAt ? ` · ${origin.retrievedAt}` : ""}
                </div>
                {origin.sourceUrl ? (
                  <a
                    href={origin.sourceUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center gap-1 break-all underline underline-offset-2"
                  >
                    Source URL ↗
                  </a>
                ) : null}
                {origin.sourceObjectKey ? (
                  <div className="text-muted-foreground font-mono break-all">
                    {origin.sourceObjectKey}
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        ))}
      </div>
    </details>
  );
}
