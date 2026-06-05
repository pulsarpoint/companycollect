import { useEffect, useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import { AriregisterTranslationActionForm } from "~/components/app/AriregisterTranslationActionForm";

type AriregisterSourceEntryAction = "" | "translation";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  selectedIds: string[];
  totalCount: number;
  filters: Record<string, string>;
  initialScope?: unknown;
  onStarted?: () => void;
}

const AVAILABLE_ACTIONS: Array<{
  key: Exclude<AriregisterSourceEntryAction, "">;
  label: string;
  description: string;
}> = [
  {
    key: "translation",
    label: "Translation",
    description:
      "Translate missing Ariregister source company fields and store English values in ariregister_source.",
  },
];

export function AriregisterSourceEntryActionSheet({
  open,
  onOpenChange,
  selectedIds,
  totalCount,
  filters,
  initialScope,
  onStarted,
}: Props) {
  const [selectedAction, setSelectedAction] =
    useState<AriregisterSourceEntryAction>("");

  useEffect(() => {
    if (open) setSelectedAction("");
  }, [open]);

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto sm:max-w-2xl">
        <SheetHeader>
          <SheetTitle>Ariregister source actions</SheetTitle>
          <SheetDescription>
            Select an action, then configure the workflow options.
          </SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-5 px-4 pb-4">
          <div className="flex flex-col gap-2">
            <label
              htmlFor="ariregister-source-action-select"
              className="text-sm font-medium"
            >
              Available action
            </label>
            <select
              id="ariregister-source-action-select"
              className="h-9 rounded-md border border-input bg-background px-3 text-sm"
              value={selectedAction}
              onChange={(event) =>
                setSelectedAction(
                  event.target.value as AriregisterSourceEntryAction,
                )
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
              <div className="flex flex-col gap-1 text-xs leading-5 text-muted-foreground">
                {AVAILABLE_ACTIONS.map((action) => (
                  <p key={action.key}>{action.description}</p>
                ))}
              </div>
            )}
          </div>

          {selectedAction === "translation" && (
            <AriregisterTranslationActionForm
              selectedIds={selectedIds}
              totalCount={totalCount}
              filters={filters}
              initialScope={initialScope}
              recordLabel="source entries"
              showAdvancedOptions
              onStarted={onStarted}
              onClose={() => onOpenChange(false)}
            />
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
