import { useEffect, useState } from "react";
import { Languages } from "lucide-react";
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

type BrregRawRecordAction = "translation";

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
  key: BrregRawRecordAction;
  label: string;
  description: string;
  icon: typeof Languages;
}> = [
  {
    key: "translation",
    label: "Translation",
    description: "Translate BRREG raw payloads into English artifacts.",
    icon: Languages,
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
  const [selectedAction, setSelectedAction] = useState<BrregRawRecordAction>("translation");

  useEffect(() => {
    if (open) setSelectedAction("translation");
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
            <div className="text-sm font-medium">Available actions</div>
            <div className="grid gap-2">
              {AVAILABLE_ACTIONS.map((action) => {
                const Icon = action.icon;
                const selected = selectedAction === action.key;
                return (
                  <button
                    key={action.key}
                    type="button"
                    className={`flex items-start gap-3 rounded-md border p-3 text-left transition-colors ${
                      selected ? "border-primary bg-primary/5" : "hover:bg-muted/50"
                    }`}
                    onClick={() => setSelectedAction(action.key)}
                  >
                    <Icon className="mt-0.5 size-4 text-muted-foreground" />
                    <span className="flex flex-col gap-1">
                      <span className="text-sm font-medium">{action.label}</span>
                      <span className="text-xs leading-5 text-muted-foreground">{action.description}</span>
                    </span>
                  </button>
                );
              })}
            </div>
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
