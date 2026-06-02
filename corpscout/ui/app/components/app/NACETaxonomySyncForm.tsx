import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Switch } from "~/components/ui/switch";

export interface NACETaxonomySyncFormValue {
  revision: string;
  source_url: string;
  force_reprocess: boolean;
}

export const defaultNACETaxonomySyncFormValue: NACETaxonomySyncFormValue = {
  revision: "2.1",
  source_url: "",
  force_reprocess: false,
};

interface NACETaxonomySyncFormProps {
  value: NACETaxonomySyncFormValue;
  onChange: (value: NACETaxonomySyncFormValue) => void;
}

export function NACETaxonomySyncForm({
  value,
  onChange,
}: NACETaxonomySyncFormProps) {
  return (
    <div className="grid gap-4">
      <div className="grid gap-2">
        <Label htmlFor="nace-revision">Revision</Label>
        <Input
          id="nace-revision"
          value={value.revision}
          onChange={(event) =>
            onChange({ ...value, revision: event.target.value })
          }
        />
        <p className="text-xs text-muted-foreground">
          Stored on imported NACE classifications and codes.
        </p>
      </div>
      <div className="grid gap-2">
        <Label htmlFor="nace-source-url">Source URL</Label>
        <Input
          id="nace-source-url"
          value={value.source_url}
          onChange={(event) =>
            onChange({ ...value, source_url: event.target.value })
          }
          placeholder="Use backend default when empty"
        />
        <p className="text-xs text-muted-foreground">
          Leave empty to use CORPSCOUT_NACE_REV21_SOURCE_URL from the scheduler.
        </p>
      </div>
      <div className="flex items-center justify-between gap-4">
        <div>
          <Label>Force reprocess</Label>
          <p className="text-xs text-muted-foreground">
            Re-import even when the downloaded source hash was already
            processed.
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
