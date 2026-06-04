import { useState } from "react";
import { Play } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { api, errorMessage } from "~/lib/api";

interface Props {
  onStarted?: () => void;
  onClose: () => void;
}

export function BrregSourceExplorerRefreshActionForm({
  onStarted,
  onClose,
}: Props) {
  const [trigger, setTrigger] = useState("manual");
  const [submitting, setSubmitting] = useState(false);

  async function submit() {
    setSubmitting(true);
    try {
      await api.refreshBrregSourceExplorer({
        trigger: trigger.trim() || "manual",
      });
      toast.success("BRREG source explorer refresh workflow started.");
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(
        errorMessage(error, "Failed to start BRREG source explorer refresh."),
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Refresh source explorer</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Rebuilds the materialized read model used by this table. Run this
          after large BRREG source normalization jobs finish.
        </p>
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="brreg-source-explorer-refresh-trigger">
          Trigger label
        </Label>
        <Input
          id="brreg-source-explorer-refresh-trigger"
          value={trigger}
          onChange={(event) => setTrigger(event.target.value)}
        />
        <p className="text-xs leading-5 text-muted-foreground">
          Stored with the Temporal workflow input so manual and scheduled
          refreshes are easy to distinguish.
        </p>
      </div>

      <Button type="button" onClick={submit} disabled={submitting}>
        <Play className="size-4" />
        {submitting ? "Starting..." : "Start refresh"}
      </Button>
    </div>
  );
}
