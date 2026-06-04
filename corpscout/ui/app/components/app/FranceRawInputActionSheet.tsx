import { useEffect, useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import { FranceBulkLoadActionForm } from "~/components/app/FranceBulkLoadActionForm";

type FranceRawInputAction = "" | "load_bulk";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onStarted?: () => void;
}

const AVAILABLE_ACTIONS: Array<{
  key: Exclude<FranceRawInputAction, "">;
  label: string;
  description: string;
}> = [
  {
    key: "load_bulk",
    label: "Load SIRENE bulk",
    description: "Load legal-unit and establishment rows from the official SIRENE parquet files.",
  },
];

export function FranceRawInputActionSheet({
  open,
  onOpenChange,
  onStarted,
}: Props) {
  const [selectedAction, setSelectedAction] = useState<FranceRawInputAction>("");

  useEffect(() => {
    if (open) setSelectedAction("");
  }, [open]);

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto sm:max-w-2xl">
        <SheetHeader>
          <SheetTitle>France actions</SheetTitle>
          <SheetDescription>Select an action, then configure the workflow options.</SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-5 px-4 pb-4">
          <div className="flex flex-col gap-2">
            <label htmlFor="france-action-select" className="text-sm font-medium">
              Available action
            </label>
            <select
              id="france-action-select"
              className="h-9 rounded-md border border-input bg-background px-3 text-sm"
              value={selectedAction}
              onChange={(event) =>
                setSelectedAction(event.target.value as FranceRawInputAction)
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
                Choose the workflow action you want to run for France raw records.
              </p>
            )}
          </div>

          {selectedAction === "load_bulk" && (
            <FranceBulkLoadActionForm
              onStarted={onStarted}
              onClose={() => onOpenChange(false)}
            />
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
