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
} from "~/components/app/BrregTranslationActionForm";
import type { BrregActionScope } from "~/components/app/BrregActionScope";

type BrregSourceEntryAction = "" | "translation" | "domain_search";

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
  key: Exclude<BrregSourceEntryAction, "">;
  label: string;
  description: string;
  disabled?: boolean;
}> = [
  {
    key: "translation",
    label: "Translation",
    description: "Translate missing BRREG source fields and store the English values in brreg_source.",
  },
  {
    key: "domain_search",
    label: "Domain discovery",
    description: "Disabled until domain discovery writes suggestions and information into brreg_source.",
    disabled: true,
  },
];

export function BrregSourceEntryActionSheet({
  open,
  onOpenChange,
  selectedIds,
  totalCount,
  filters,
  initialScope,
  onStarted,
}: Props) {
  const [selectedAction, setSelectedAction] = useState<BrregSourceEntryAction>("");

  useEffect(() => {
    if (open) setSelectedAction("");
  }, [open]);

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto sm:max-w-2xl">
        <SheetHeader>
          <SheetTitle>BRREG source actions</SheetTitle>
          <SheetDescription>Select an action, then configure the workflow options.</SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-5 px-4 pb-4">
          <div className="flex flex-col gap-2">
            <label htmlFor="brreg-source-action-select" className="text-sm font-medium">
              Available action
            </label>
            <select
              id="brreg-source-action-select"
              className="h-9 rounded-md border border-input bg-background px-3 text-sm"
              value={selectedAction}
              onChange={(event) => setSelectedAction(event.target.value as BrregSourceEntryAction)}
            >
              <option value="">Select action</option>
              {AVAILABLE_ACTIONS.map((action) => (
                <option key={action.key} value={action.key} disabled={action.disabled}>
                  {action.disabled ? `${action.label} (disabled)` : action.label}
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
            <BrregTranslationActionForm
              selectedIds={selectedIds}
              totalCount={totalCount}
              filters={filters}
              initialScope={initialScope}
              recordLabel="source entries"
              description="Starts the Temporal workflow that claims missing BRREG source fields, sends field payloads to the translation service, and stores translated values in brreg_source."
              showAdvancedOptions={false}
              onStarted={onStarted}
              onClose={() => onOpenChange(false)}
            />
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
