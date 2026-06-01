import { formatDate, signalColor, confidenceColor } from "~/lib/utils";
import type { ReviewCandidate } from "~/types/api";
import { Button } from "~/components/ui/button";
import { Badge } from "~/components/ui/badge";
import { Separator } from "~/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";

interface ReviewSheetProps {
  candidate: ReviewCandidate | null;
  onClose: () => void;
  onAction: (id: string, action: "approved" | "rejected" | "superseded") => Promise<void>;
  loading?: boolean;
}

export function ReviewSheet({ candidate, onClose, onAction, loading }: ReviewSheetProps) {
  if (!candidate) return null;

  return (
    <Sheet open={candidate != null} onOpenChange={(open) => !open && onClose()}>
      <SheetContent className="w-[400px] sm:w-[540px] overflow-y-auto">
        <SheetHeader>
          <SheetTitle>{candidate.company_name}</SheetTitle>
          <SheetDescription>Domain candidate details</SheetDescription>
        </SheetHeader>

        <div className="mt-6 space-y-4">
          <div>
            <p className="text-xs text-muted-foreground uppercase tracking-wide">Domain</p>
            <p className="mt-1 font-mono text-lg text-primary">{candidate.domain}</p>
          </div>

          <div className="flex gap-4">
            <div>
              <p className="text-xs text-muted-foreground uppercase tracking-wide">Signal</p>
              <Badge className={`mt-1 ${signalColor(candidate.signal)}`} variant="outline">
                {candidate.signal}
              </Badge>
            </div>
            <div>
              <p className="text-xs text-muted-foreground uppercase tracking-wide">Confidence</p>
              <p className={`mt-1 font-bold text-lg ${confidenceColor(candidate.confidence)}`}>
                {candidate.confidence}
              </p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground uppercase tracking-wide">Relationship</p>
              <p className="mt-1 text-sm">{candidate.relationship_type}</p>
            </div>
          </div>

          <div className="flex gap-4">
            <div>
              <p className="text-xs text-muted-foreground uppercase tracking-wide">First seen</p>
              <p className="mt-1 text-sm">{formatDate(candidate.first_seen_at)}</p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground uppercase tracking-wide">Last seen</p>
              <p className="mt-1 text-sm">{formatDate(candidate.last_seen_at)}</p>
            </div>
          </div>

          {candidate.evidence && (
            <div>
              <p className="text-xs text-muted-foreground uppercase tracking-wide mb-1">Evidence</p>
              <pre className="rounded-md bg-muted p-3 text-xs overflow-x-auto whitespace-pre-wrap break-all">
                {JSON.stringify(candidate.evidence, null, 2)}
              </pre>
            </div>
          )}

          <Separator />

          <div className="flex gap-2">
            <Button
              className="flex-1"
              variant="default"
              disabled={loading}
              onClick={() => onAction(candidate.id, "approved")}
            >
              Approve
            </Button>
            <Button
              className="flex-1"
              variant="destructive"
              disabled={loading}
              onClick={() => onAction(candidate.id, "rejected")}
            >
              Reject
            </Button>
            <Button
              variant="outline"
              disabled={loading}
              onClick={() => onAction(candidate.id, "superseded")}
            >
              Supersede
            </Button>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
}
