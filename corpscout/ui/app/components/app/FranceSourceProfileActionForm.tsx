import { useEffect, useState } from "react";
import { DatabaseZap } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { api, errorMessage } from "~/lib/api";

type SourceProfileMode = "limit" | "all";

interface Props {
  onStarted?: () => void;
  onClose: () => void;
}

function parsePositiveNumber(value: string) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.floor(parsed);
}

export function FranceSourceProfileActionForm({ onStarted, onClose }: Props) {
  const [mode, setMode] = useState<SourceProfileMode>("limit");
  const [limit, setLimit] = useState("1000");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (mode === "limit" && limit.trim() === "") setLimit("1000");
  }, [limit, mode]);

  const limitInvalid = mode === "limit" && parsePositiveNumber(limit) === null;
  const effectiveLimit = mode === "all" ? 0 : parsePositiveNumber(limit);
  const canSubmit = effectiveLimit !== null;

  async function submit() {
    if (!canSubmit || effectiveLimit === null) return;

    setSubmitting(true);
    try {
      await api.loadFranceSourceProfiles({
        limit: effectiveLimit,
        trigger: "manual",
      });
      toast.success("France source profile build workflow started.");
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(errorMessage(error, "Failed to start France source profile build."));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Build France source profile</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Parse current SIRENE raw legal units and establishments into france_source tables.
        </p>
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="france-source-profile-mode">Records to build</Label>
        <select
          id="france-source-profile-mode"
          className="h-9 rounded-md border border-input bg-background px-3 text-sm"
          value={mode}
          onChange={(event) => setMode(event.target.value as SourceProfileMode)}
        >
          <option value="limit">Limited number</option>
          <option value="all">All records</option>
        </select>
        <p className="text-xs leading-5 text-muted-foreground">
          Use a limited number while validating the pipeline; use all to build profiles from every current SIRENE raw legal unit that is missing or changed.
        </p>
      </div>

      {mode === "limit" && (
        <div className="flex flex-col gap-2">
          <Label htmlFor="france-source-profile-limit">Limit</Label>
          <Input
            id="france-source-profile-limit"
            type="number"
            min={1}
            value={limit}
            onChange={(event) => setLimit(event.target.value)}
          />
          <p className="text-xs leading-5 text-muted-foreground">
            Maximum number of current SIRENE legal-unit rows to parse.
          </p>
          {limitInvalid && (
            <p className="text-xs leading-5 text-destructive">
              Limit must be a positive number.
            </p>
          )}
        </div>
      )}

      <div className="flex justify-end gap-2">
        <Button variant="outline" type="button" onClick={onClose}>
          Cancel
        </Button>
        <Button type="button" disabled={!canSubmit || submitting} onClick={submit}>
          <DatabaseZap className="mr-2 h-4 w-4" />
          {submitting ? "Starting..." : "Start source profile build"}
        </Button>
      </div>
    </div>
  );
}
