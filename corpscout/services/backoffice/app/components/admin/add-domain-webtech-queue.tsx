import { useState } from "react";
import { useFetcher } from "react-router";
import { PlusIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import type { action } from "~/routes/admin-se-domain-web-technologies";
import { useQueueSubmission, QueueImportStatus } from "~/components/admin/queue-import-status";

export function AddDomainWebtechQueue({domain, actionPath}: {domain: string; actionPath: string}) {
  const submit = useFetcher<typeof action>();
  const [submissionId] = useState(() => crypto.randomUUID());
  const receipt = submit.data?.ok ? submit.data : null;
  const {state, error: statusError} = useQueueSubmission(receipt);
  const completed = state?.status === "SUCCESS";
  const failed = state?.status === "FAILURE" || state?.status === "CANCELED";
  const waiting = Boolean(receipt && !state?.finished);
  const busy = submit.state !== "idle";
  const error = submit.data?.ok === false ? submit.data.error : statusError;

  return <div className="flex flex-col gap-3">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <p className="text-sm text-muted-foreground">Queue the homepage of {domain}. Start the scan separately from Queues.</p>
      <Button disabled={busy || waiting || completed} onClick={() => submit.submit({intent: "add-webtech-input", submissionId}, {method: "post", action: actionPath})}>
        <PlusIcon data-icon="inline-start" />{busy || waiting ? "Adding to queue…" : completed ? "Added to Webtech queue" : failed ? "Retry adding to queue" : "Add to Webtech queue"}
      </Button>
    </div>
    {receipt && <QueueImportStatus receipt={receipt} state={state} fallbackSearch={domain} />}
    {error && <Alert variant="destructive"><AlertTitle>Queue request needs attention</AlertTitle><AlertDescription>{error}</AlertDescription></Alert>}
  </div>;
}
