import { useEffect, useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import { SEBulkLoadActionForm } from "~/components/app/SEBulkLoadActionForm";

type SERawInputAction = "" | "load_bulk";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onStarted?: () => void;
}

const AVAILABLE_ACTIONS: Array<{
  key: Exclude<SERawInputAction, "">;
  label: string;
  description: string;
}> = [
  {
    key: "load_bulk",
    label: "Load HVD bulk",
    description: "Load Swedish HVD organization rows from configured bulk files.",
  },
];

export function SERawInputActionSheet({
  open,
  onOpenChange,
  onStarted,
}: Props) {
  const [selectedAction, setSelectedAction] = useState<SERawInputAction>("");

  useEffect(() => {
    if (open) setSelectedAction("");
  }, [open]);

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto sm:max-w-2xl">
        <SheetHeader>
          <SheetTitle>Sweden HVD actions</SheetTitle>
          <SheetDescription>Select an action, then configure the workflow options.</SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-5 px-4 pb-4">
          <div className="flex flex-col gap-2">
            <label htmlFor="se-action-select" className="text-sm font-medium">
              Available action
            </label>
            <select
              id="se-action-select"
              className="h-9 rounded-md border border-input bg-background px-3 text-sm"
              value={selectedAction}
              onChange={(event) =>
                setSelectedAction(event.target.value as SERawInputAction)
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
                Choose the workflow action you want to run for Sweden HVD raw records.
              </p>
            )}
          </div>

          {selectedAction === "load_bulk" && (
            <SEBulkLoadActionForm
              onStarted={onStarted}
              onClose={() => onOpenChange(false)}
            />
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
