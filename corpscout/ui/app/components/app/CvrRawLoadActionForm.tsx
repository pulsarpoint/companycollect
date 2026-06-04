import { useEffect, useState } from "react";
import { DatabaseZap } from "lucide-react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { api, errorMessage } from "~/lib/api";

type RawLoadMode = "limit" | "all";

interface Props {
  onStarted?: () => void;
  onClose: () => void;
}

function parsePositiveNumber(value: string) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.floor(parsed);
}

export function CvrRawLoadActionForm({ onStarted, onClose }: Props) {
  const [mode, setMode] = useState<RawLoadMode>("limit");
  const [limit, setLimit] = useState("100");
  const [pageSize, setPageSize] = useState("100");
  const [scroll, setScroll] = useState("1m");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (mode === "limit" && limit.trim() === "") setLimit("100");
  }, [limit, mode]);

  const parsedLimit = mode === "all" ? 0 : parsePositiveNumber(limit);
  const parsedPageSize = parsePositiveNumber(pageSize);
  const limitInvalid = mode === "limit" && parsedLimit === null;
  const pageSizeInvalid = parsedPageSize === null;
  const scrollInvalid = scroll.trim() === "";
  const canSubmit = parsedLimit !== null && parsedPageSize !== null && !scrollInvalid;

  async function submit() {
    if (!canSubmit || parsedLimit === null || parsedPageSize === null) return;

    setSubmitting(true);
    try {
      await api.loadCVRRawRecords({
        limit: parsedLimit,
        page_size: parsedPageSize,
        scroll: scroll.trim(),
        trigger: "manual",
      });
      toast.success(
        mode === "all"
          ? "Full CVR raw ingest workflow started."
          : "CVR raw ingest workflow started.",
      );
      onStarted?.();
      onClose();
    } catch (error) {
      toast.error(errorMessage(error, "Failed to start CVR raw ingest."));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="rounded-md border bg-muted/20 p-3">
        <div className="text-sm font-medium">Load CVR raw records</div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Load Danish CVR Elasticsearch records into cvr_workflow.raw_records.
        </p>
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="cvr-raw-load-mode">Records to load</Label>
        <select
          id="cvr-raw-load-mode"
          className="h-9 rounded-md border border-input bg-background px-3 text-sm"
          value={mode}
          onChange={(event) => setMode(event.target.value as RawLoadMode)}
        >
          <option value="limit">Limited number</option>
          <option value="all">All records</option>
        </select>
        <p className="text-xs leading-5 text-muted-foreground">
          Start with a limited sample while validating the CVR structure; use all
          only after credentials and storage are confirmed.
        </p>
      </div>

      {mode === "limit" && (
        <div className="flex flex-col gap-2">
          <Label htmlFor="cvr-raw-load-limit">Limit</Label>
          <Input
            id="cvr-raw-load-limit"
            type="number"
            min={1}
            value={limit}
            onChange={(event) => setLimit(event.target.value)}
          />
          {limitInvalid && (
            <p className="text-xs leading-5 text-destructive">
              Limit must be a positive number.
            </p>
          )}
        </div>
      )}

      <div className="flex flex-col gap-2">
        <Label htmlFor="cvr-raw-load-page-size">Page size</Label>
        <Input
          id="cvr-raw-load-page-size"
          type="number"
          min={1}
          value={pageSize}
          onChange={(event) => setPageSize(event.target.value)}
        />
        {pageSizeInvalid && (
          <p className="text-xs leading-5 text-destructive">
            Page size must be a positive number.
          </p>
        )}
      </div>

      <div className="flex flex-col gap-2">
        <Label htmlFor="cvr-raw-load-scroll">Scroll</Label>
        <Input
          id="cvr-raw-load-scroll"
          value={scroll}
          onChange={(event) => setScroll(event.target.value)}
        />
        {scrollInvalid && (
          <p className="text-xs leading-5 text-destructive">
            Scroll value is required.
          </p>
        )}
      </div>

      <div className="flex justify-end gap-2">
        <Button variant="outline" type="button" onClick={onClose}>
          Cancel
        </Button>
        <Button type="button" disabled={!canSubmit || submitting} onClick={submit}>
          <DatabaseZap className="mr-2 h-4 w-4" />
          {submitting ? "Starting..." : "Start raw ingest"}
        </Button>
      </div>
    </div>
  );
}
