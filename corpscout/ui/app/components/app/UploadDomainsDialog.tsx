import { useRef, useState } from "react";
import { toast } from "sonner";
import { Button } from "~/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "~/components/ui/dialog";
import { uploadDomainsCSV } from "~/lib/api";
import type { DomainImportBatch } from "~/types/api";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSuccess?: (batch: DomainImportBatch) => void;
}

export function UploadDomainsDialog({ open, onOpenChange, onSuccess }: Props) {
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    setFile(e.target.files?.[0] ?? null);
  }

  async function handleSubmit() {
    if (!file) return;
    setLoading(true);
    try {
      const batch = await uploadDomainsCSV(file);
      toast.success(`Import started — batch ${batch.id.slice(0, 8)}…`);
      onOpenChange(false);
      onSuccess?.(batch);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Upload failed.");
    } finally {
      setLoading(false);
    }
  }

  function handleOpenChange(v: boolean) {
    if (!v) {
      setFile(null);
      if (inputRef.current) inputRef.current.value = "";
    }
    onOpenChange(v);
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Upload Domains CSV</DialogTitle>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <p className="text-sm text-muted-foreground">
            CSV must have a header row and columns in this order:
          </p>
          <pre className="rounded bg-muted px-3 py-2 text-xs font-mono">
            num,domain,company{"\n"}
            1,example.com,Acme Corp{"\n"}
            2,orphan.io,
          </pre>
          <p className="text-sm text-muted-foreground">
            <strong>company</strong> is optional. When provided, the company is looked up by exact
            name and linked to the domain if found. Unrecognised company names are ignored — the
            domain is still imported.
          </p>
          <input
            ref={inputRef}
            type="file"
            accept=".csv,text/csv"
            className="block w-full text-sm text-foreground file:mr-3 file:rounded file:border file:border-input file:bg-background file:px-3 file:py-1 file:text-sm file:font-medium"
            onChange={handleFileChange}
          />
          {file && (
            <p className="text-xs text-muted-foreground">
              {file.name} ({(file.size / 1024).toFixed(1)} KB)
            </p>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => handleOpenChange(false)} disabled={loading}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={!file || loading}>
            {loading ? "Uploading…" : "Upload"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
