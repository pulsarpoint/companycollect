import { useId } from "react";
import { useFetcher } from "react-router";
import { CheckCircle2, Link2, LoaderCircle, RotateCcw, XCircle } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Field, FieldDescription, FieldLabel } from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import type { CompanyDomainReviewStatus } from "~/lib/company-domains.server";

type ReviewActionData = { ok: true } | { ok: false; error: string };

export function DomainReviewControls({
  rootDomain,
  reviewStatus,
  confidenceOverride,
  action,
}: {
  rootDomain: string;
  reviewStatus: CompanyDomainReviewStatus;
  confidenceOverride?: number | null;
  action: string;
}) {
  const confidenceId = useId();
  const fetcher = useFetcher<ReviewActionData>();
  const submitting = fetcher.state !== "idle";

  return (
    <div className="flex flex-col gap-3">
      <fetcher.Form
        method="post"
        action={action}
        className="flex flex-wrap gap-2"
      >
        <input type="hidden" name="root_domain" value={rootDomain} />
        <Button
          type="submit"
          name="review_status"
          value="confirmed_primary"
          size="sm"
          variant={
            reviewStatus === "confirmed_primary" ? "default" : "outline"
          }
          disabled={submitting || reviewStatus === "confirmed_primary"}
        >
          {submitting ? (
            <LoaderCircle data-icon="inline-start" className="animate-spin" />
          ) : (
            <CheckCircle2 data-icon="inline-start" />
          )}
          Confirm primary
        </Button>
        <Button
          type="submit"
          name="review_status"
          value="confirmed_related"
          size="sm"
          variant={
            reviewStatus === "confirmed_related"
              ? "secondary"
              : "outline"
          }
          disabled={submitting || reviewStatus === "confirmed_related"}
        >
          <Link2 data-icon="inline-start" />
          Confirm related
        </Button>
        <Button
          type="submit"
          name="review_status"
          value="rejected"
          size="sm"
          variant={
            reviewStatus === "rejected" ? "destructive" : "outline"
          }
          disabled={submitting || reviewStatus === "rejected"}
        >
          <XCircle data-icon="inline-start" />
          Doesn’t belong
        </Button>
        {reviewStatus !== "unreviewed" || confidenceOverride != null ? (
          <Button
            type="submit"
            name="review_status"
            value="unreviewed"
            size="sm"
            variant="ghost"
            disabled={submitting}
          >
            <RotateCcw data-icon="inline-start" />
            Clear review
          </Button>
        ) : null}
      </fetcher.Form>

      <p className="text-sm text-muted-foreground">“Doesn’t belong” excludes this company/domain pair until you clear the review, even when a source proposes it again.</p>
      <fetcher.Form method="post" action={action} className="flex flex-wrap items-end gap-3">
        <input type="hidden" name="root_domain" value={rootDomain} />
        <input type="hidden" name="review_status" value={reviewStatus} />
        <Field className="max-w-sm">
          <FieldLabel htmlFor={confidenceId}>Confidence override (0–1)</FieldLabel>
          <Input key={`${rootDomain}:${confidenceOverride}`} id={confidenceId} name="confidence_override"
            type="number" min={0} max={1} step="0.01" defaultValue={confidenceOverride ?? ""}
            placeholder="Use source score" disabled={submitting} />
          <FieldDescription>Leave empty to use the calculated score. Source evidence keeps its original confidence.</FieldDescription>
        </Field>
        <Button type="submit" variant="outline" size="sm" disabled={submitting}>Save confidence</Button>
      </fetcher.Form>

      {fetcher.data && !fetcher.data.ok ? (
        <Alert variant="destructive">
          <AlertTitle>Review was not saved</AlertTitle>
          <AlertDescription>{fetcher.data.error}</AlertDescription>
        </Alert>
      ) : null}
    </div>
  );
}
