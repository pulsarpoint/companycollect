import { useEffect, useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import type { BrregActionScope } from "~/components/app/BrregActionScope";
import { BrregBulkLoadActionForm } from "~/components/app/BrregBulkLoadActionForm";
import { BrregSourceProfileNormalizationActionForm } from "~/components/app/BrregSourceProfileNormalizationActionForm";

type BrregRawRecordAction = "" | "load_bulk" | "sync_source" | "sync_api";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  selectedIds: string[];
  totalCount: number;
  filters: Record<string, string>;
  initialScope?: BrregActionScope;
  onStarted?: () => void;
}

const AVAILABLE_ACTIONS: Array<{
  key: Exclude<BrregRawRecordAction, "">;
  label: string;
  description: string;
  disabled?: boolean;
}> = [
  {
    key: "load_bulk",
    label: "Load bulk",
    description: "Load raw BRREG entries from the bulk file.",
  },
  {
    key: "sync_source",
    label: "Sync to brreg_source",
    description: "Upsert raw entries into normalized BRREG source tables.",
  },
  {
    key: "sync_api",
    label: "Sync API",
    description: "Fetch updates from the BRREG API. Not implemented yet.",
    disabled: true,
  },
];

export function BrregRawRecordActionSheet({
  open,
  onOpenChange,
  selectedIds,
  totalCount,
  filters,
  initialScope,
  onStarted,
}: Props) {
  const [selectedAction, setSelectedAction] = useState<BrregRawRecordAction>("");

  useEffect(() => {
    if (open) setSelectedAction("");
  }, [open]);

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto sm:max-w-2xl">
        <SheetHeader>
          <SheetTitle>BRREG actions</SheetTitle>
          <SheetDescription>Select a raw ingest or source sync action.</SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-5 px-4 pb-4">
          <div className="flex flex-col gap-2">
            <label htmlFor="brreg-action-select" className="text-sm font-medium">
              Available action
            </label>
            <select
              id="brreg-action-select"
              className="h-9 rounded-md border border-input bg-background px-3 text-sm"
              value={selectedAction}
              onChange={(event) => setSelectedAction(event.target.value as BrregRawRecordAction)}
            >
              <option value="">Select action</option>
              {AVAILABLE_ACTIONS.map((action) => (
                <option key={action.key} value={action.key} disabled={action.disabled}>
                  {action.label}
                </option>
              ))}
            </select>
            {selectedAction === "" && (
              <p className="text-xs leading-5 text-muted-foreground">
                Choose the workflow action you want to run for these BRREG raw records.
              </p>
            )}
          </div>

          {selectedAction === "load_bulk" && (
            <BrregBulkLoadActionForm
              onStarted={onStarted}
              onClose={() => onOpenChange(false)}
            />
          )}

          {selectedAction === "sync_source" && (
            <BrregSourceProfileNormalizationActionForm
              selectedIds={selectedIds}
              totalCount={totalCount}
              filters={filters}
              initialScope={initialScope}
              onStarted={onStarted}
              onClose={() => onOpenChange(false)}
            />
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
