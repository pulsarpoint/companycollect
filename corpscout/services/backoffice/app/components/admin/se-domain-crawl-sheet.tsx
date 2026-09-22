import { useState } from "react";
import { PlayIcon } from "lucide-react";
import { DOMAIN_CRAWL_TYPES, type DomainCrawlType } from "~/lib/se-domain-selection";
import { CrawlSettingsFields } from "~/components/admin/crawl-settings-fields";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Button } from "~/components/ui/button";
import { Field, FieldLabel } from "~/components/ui/field";
import { NativeSelect, NativeSelectOption } from "~/components/ui/native-select";
import { Sheet, SheetContent, SheetDescription, SheetFooter, SheetHeader, SheetTitle } from "~/components/ui/sheet";

/**
 * "Send for crawl": choose a crawl type and its execution settings, then launch the
 * Dagster workflow that freezes the selection as a task and crawls all of it.
 */
export function SeDomainCrawlSheet({open, selectedCount, busy, error, onClose, onSubmit}: {
  open: boolean; selectedCount: number; busy: boolean; error: string | null;
  onClose: () => void; onSubmit: (crawlType: DomainCrawlType, settings: Record<string, string>) => void;
}) {
  const [type, setType] = useState<DomainCrawlType>("site_info");
  const noun = selectedCount === 1 ? "domain" : "domains";
  return <Sheet open={open} onOpenChange={(next) => {if (!next && !busy) onClose();}}>
    <SheetContent className="data-[side=right]:w-full data-[side=right]:sm:max-w-xl" showCloseButton={!busy}>
      <SheetHeader><SheetTitle>Send for crawl</SheetTitle>
        <SheetDescription>Crawl {selectedCount.toLocaleString()} selected {noun}. Dagster saves them as inputs, freezes this selection as one task and crawls every enabled domain of it.</SheetDescription>
      </SheetHeader>
      <form className="flex min-h-0 flex-1 flex-col" onSubmit={(event) => {
        event.preventDefault();
        const form = new FormData(event.currentTarget);
        form.delete("crawl_type");
        onSubmit(type, Object.fromEntries([...form.entries()].map(([key, value]) => [key, String(value)])));
      }}>
        <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto px-4 pb-4">
          <Field><FieldLabel htmlFor="send-crawl-type">Crawl type</FieldLabel>
            <NativeSelect id="send-crawl-type" name="crawl_type" value={type} onChange={(event) => setType(event.target.value as DomainCrawlType)}>
              {DOMAIN_CRAWL_TYPES.map((option) => <NativeSelectOption key={option.value} value={option.value}>{option.label}</NativeSelectOption>)}
            </NativeSelect></Field>
          <CrawlSettingsFields key={type} type={type} idPrefix="send" />
          <p className="text-xs text-muted-foreground">Existing inputs keep their saved browser mode, proxy route and artifact settings. Disabled inputs are skipped. Recent successful results are skipped unless you force a new crawl.</p>
          {error && <Alert variant="destructive"><AlertTitle>Could not send for crawl</AlertTitle><AlertDescription>{error}</AlertDescription></Alert>}
        </div>
        <SheetFooter className="border-t">
          <Button type="submit" disabled={busy || selectedCount === 0}><PlayIcon data-icon="inline-start" />{busy ? "Sending…" : `Crawl ${selectedCount.toLocaleString()} ${noun}`}</Button>
          <Button type="button" variant="outline" disabled={busy} onClick={onClose}>Cancel</Button>
        </SheetFooter>
      </form>
    </SheetContent>
  </Sheet>;
}
