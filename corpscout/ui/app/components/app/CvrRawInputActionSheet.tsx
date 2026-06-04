import { useEffect, useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import { CvrRawLoadActionForm } from "~/components/app/CvrRawLoadActionForm";

type CvrRawInputAction = "" | "load_raw";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onStarted?: () => void;
}

const AVAILABLE_ACTIONS: Array<{
  key: Exclude<CvrRawInputAction, "">;
  label: string;
  description: string;
}> = [
  {
    key: "load_raw",
    label: "Load raw records",
    description: "Load raw CVR entries from the Elasticsearch scroll endpoint.",
  },
];

export function CvrRawInputActionSheet({
  open,
  onOpenChange,
  onStarted,
}: Props) {
  const [selectedAction, setSelectedAction] = useState<CvrRawInputAction>("");

  useEffect(() => {
    if (open) setSelectedAction("");
  }, [open]);

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto sm:max-w-2xl">
        <SheetHeader>
          <SheetTitle>CVR actions</SheetTitle>
          <SheetDescription>Select a raw ingest action.</SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-5 px-4 pb-4">
          <div className="flex flex-col gap-2">
            <label htmlFor="cvr-action-select" className="text-sm font-medium">
              Available action
            </label>
            <select
              id="cvr-action-select"
              className="h-9 rounded-md border border-input bg-background px-3 text-sm"
              value={selectedAction}
              onChange={(event) =>
                setSelectedAction(event.target.value as CvrRawInputAction)
              }
            >
              <option value="">Select action</option>
              {AVAILABLE_ACTIONS.map((action) => (
                <option key={action.key} value={action.key}>
                  {action.label}
                </option>
              ))}
            </select>
            {selectedAction === "" && (
              <p className="text-xs leading-5 text-muted-foreground">
                Choose the workflow action you want to run for CVR raw records.
              </p>
            )}
          </div>

          {selectedAction === "load_raw" && (
            <CvrRawLoadActionForm
              onStarted={onStarted}
              onClose={() => onOpenChange(false)}
            />
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
