import { useEffect, useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import {
  BrregTranslationActionForm,
  type BrregActionScope,
} from "~/components/app/BrregTranslationActionForm";

type BrregRawRecordAction = "" | "translation";

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
}> = [
  {
    key: "translation",
    label: "Translation",
    description: "Translate BRREG raw payloads into English artifacts.",
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
          <SheetDescription>Select an action, then configure the options for that workflow.</SheetDescription>
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
                <option key={action.key} value={action.key}>
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

          {selectedAction === "translation" && (
            <BrregTranslationActionForm
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
