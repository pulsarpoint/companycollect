import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Switch } from "~/components/ui/switch";

export interface FXRateSyncFormValue {
  provider: "ecb";
  source_url: string;
  force_reprocess: boolean;
}

export const defaultFXRateSyncFormValue: FXRateSyncFormValue = {
  provider: "ecb",
  source_url: "",
  force_reprocess: false,
};

interface FXRateSyncFormProps {
  value: FXRateSyncFormValue;
  onChange: (value: FXRateSyncFormValue) => void;
}

export function FXRateSyncForm({ value, onChange }: FXRateSyncFormProps) {
  return (
    <div className="grid gap-4">
      <div className="grid gap-2">
        <Label htmlFor="fx-provider">Provider</Label>
        <Input id="fx-provider" value={value.provider} disabled />
        <p className="text-xs text-muted-foreground">
          ECB daily reference rates are the supported provider for this sync.
        </p>
      </div>
      <div className="grid gap-2">
        <Label htmlFor="fx-source-url">Source URL</Label>
        <Input
          id="fx-source-url"
          value={value.source_url}
          onChange={(event) =>
            onChange({ ...value, source_url: event.target.value })
          }
          placeholder="Use backend default when empty"
        />
        <p className="text-xs text-muted-foreground">
          Leave empty to use CORPSCOUT_FX_ECB_DAILY_URL from the scheduler.
        </p>
      </div>
      <div className="flex items-center justify-between gap-4">
        <div>
          <Label>Force reprocess</Label>
          <p className="text-xs text-muted-foreground">
            Re-import even when the downloaded exchange-rate source hash was
            already processed.
          </p>
        </div>
        <Switch
          checked={value.force_reprocess}
          onCheckedChange={(checked) =>
            onChange({ ...value, force_reprocess: checked })
          }
        />
      </div>
    </div>
  );
}
