import { useEffect, useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import { AriregisterBulkLoadActionForm } from "~/components/app/AriregisterBulkLoadActionForm";
import { AriregisterSourceProfileActionForm } from "~/components/app/AriregisterSourceProfileActionForm";

type AriregisterRawInputAction = "" | "load_bulk" | "build_source_profile";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onStarted?: () => void;
}

const AVAILABLE_ACTIONS: Array<{
  key: Exclude<AriregisterRawInputAction, "">;
  label: string;
  description: string;
}> = [
  {
    key: "load_bulk",
    label: "Load general data",
    description: "Load raw Ariregister entries from the general-data JSON file.",
  },
  {
    key: "build_source_profile",
    label: "Build source profile",
    description: "Parse current raw records into ariregister_source tables.",
  },
];

export function AriregisterRawInputActionSheet({
  open,
  onOpenChange,
  onStarted,
}: Props) {
  const [selectedAction, setSelectedAction] =
    useState<AriregisterRawInputAction>("");

  useEffect(() => {
    if (open) setSelectedAction("");
  }, [open]);

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto sm:max-w-2xl">
        <SheetHeader>
          <SheetTitle>Ariregister actions</SheetTitle>
          <SheetDescription>Select a raw ingest action.</SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-5 px-4 pb-4">
          <div className="flex flex-col gap-2">
            <label htmlFor="ariregister-action-select" className="text-sm font-medium">
              Available action
            </label>
            <select
              id="ariregister-action-select"
              className="h-9 rounded-md border border-input bg-background px-3 text-sm"
              value={selectedAction}
              onChange={(event) =>
                setSelectedAction(event.target.value as AriregisterRawInputAction)
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
                Choose the workflow action you want to run for Ariregister raw records.
              </p>
            )}
          </div>

          {selectedAction === "load_bulk" && (
            <AriregisterBulkLoadActionForm
              onStarted={onStarted}
              onClose={() => onOpenChange(false)}
            />
          )}

          {selectedAction === "build_source_profile" && (
            <AriregisterSourceProfileActionForm
              onStarted={onStarted}
              onClose={() => onOpenChange(false)}
            />
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
