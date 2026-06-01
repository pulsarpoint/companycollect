import { useEffect, useMemo, useState } from "react";
import { Plus, RotateCcw, Save } from "lucide-react";
import type { DataSource } from "~/types/api";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import { Input } from "~/components/ui/input";

type SourcePatch = Parameters<typeof import("~/lib/api").api.patchSource>[1];

interface ConfigRow {
  id: string;
  key: string;
  value: string;
  isExisting: boolean;
  originalValue?: string;
  error?: string;
}

interface ConfigTabProps {
  source: DataSource;
  saving: boolean;
  onPatch: (patch: SourcePatch) => Promise<void>;
}

function configToRows(config: Record<string, unknown>): ConfigRow[] {
  return Object.entries(config).map(([key, value], index) => ({
    id: `${key}-${index}`,
    key,
    value: JSON.stringify(value),
    isExisting: true,
    originalValue: JSON.stringify(value),
  }));
}

function newRow(): ConfigRow {
  return { id: crypto.randomUUID(), key: "", value: "null", isExisting: false };
}

export function ConfigTab({ source, saving, onPatch }: ConfigTabProps) {
  const initialRows = useMemo(() => configToRows(source.config), [source.config]);
  const [rows, setRows] = useState<ConfigRow[]>(initialRows);

  useEffect(() => {
    setRows(initialRows);
  }, [initialRows]);

  function updateRow(id: string, patch: Partial<ConfigRow>) {
    setRows((current) => current.map((row) => row.id === id ? { ...row, ...patch, error: undefined } : row));
  }

  async function saveConfig() {
    let hasError = false;
    const nextConfig: Record<string, unknown> = {};
    const seenKeys = new Set<string>();

    const validatedRows = rows.map((row) => {
      const key = row.key.trim();
      if (!key) {
        hasError = true;
        return { ...row, error: "Key is required." };
      }
      if (row.isExisting && row.value === row.originalValue) {
        seenKeys.add(key);
        return { ...row, key };
      }
      if (/key|secret|token|password/i.test(key)) {
        hasError = true;
        return { ...row, error: "Secret-like config keys cannot be edited here." };
      }
      if (seenKeys.has(key)) {
        hasError = true;
        return { ...row, error: "Duplicate key." };
      }

      try {
        const parsedValue = JSON.parse(row.value);
        if (!row.isExisting || row.value !== row.originalValue) {
          nextConfig[key] = parsedValue;
        }
        seenKeys.add(key);
        return { ...row, key };
      } catch {
        hasError = true;
        return { ...row, error: "Value must be valid JSON." };
      }
    });

    setRows(validatedRows);
    if (hasError) return;
    if (Object.keys(nextConfig).length === 0) return;

    await onPatch({ config: nextConfig });
  }

  return (
    <Card>
      <CardHeader className="gap-3 sm:flex sm:flex-row sm:items-center sm:justify-between">
        <CardTitle>Config</CardTitle>
        <div className="flex flex-wrap gap-2">
          <Button size="sm" variant="outline" onClick={() => setRows((current) => [...current, newRow()])}>
            <Plus className="size-4" />
            Add field
          </Button>
          <Button size="sm" variant="outline" onClick={() => setRows(initialRows)}>
            <RotateCcw className="size-4" />
            Reset
          </Button>
          <Button size="sm" disabled={saving} onClick={saveConfig}>
            <Save className="size-4" />
            Save
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        {rows.length === 0 ? (
          <p className="text-sm text-muted-foreground">No editable config fields.</p>
        ) : (
          rows.map((row) => (
            <div key={row.id} className="grid gap-2 rounded-md border p-3 md:grid-cols-[minmax(160px,240px)_1fr]">
              <Input
                value={row.key}
                onChange={(event) => updateRow(row.id, { key: event.target.value })}
                placeholder="field_name"
                aria-invalid={Boolean(row.error)}
                readOnly={row.isExisting}
                className={row.isExisting ? "bg-muted font-mono text-muted-foreground" : undefined}
              />
              <Input
                value={row.value}
                onChange={(event) => updateRow(row.id, { value: event.target.value })}
                placeholder='"value"'
                aria-invalid={Boolean(row.error)}
                className="font-mono"
              />
              {row.error && (
                <p className="text-sm text-destructive md:col-span-2">{row.error}</p>
              )}
            </div>
          ))
        )}
      </CardContent>
    </Card>
  );
}
