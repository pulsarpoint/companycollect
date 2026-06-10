import { useEffect, useMemo, useState } from "react";
import { Download, FileDown, RefreshCw, Search, Upload } from "lucide-react";
import { toast } from "sonner";

import { api, errorMessage } from "~/lib/api";
import { cn, formatDate } from "~/lib/utils";
import type {
  DataSource,
  SourceAction,
  SourceActionRun,
  SourceFileRun,
  SourceFileStatus,
  SourceRunStatus,
  SourceRunTemporalStatus,
} from "~/types/api";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Skeleton } from "~/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";

interface ActionsTabProps {
  source: DataSource;
}

type TriggerKey = "pull_source" | "import_clickhouse" | "refresh_explorer_cache" | "sync";

async function loadSourceActionData(sourceName: string, selectedFileKey?: string) {
  const [loadedActions, loadedRuns, loadedFiles] = await Promise.all([
    api.getSourceActions(sourceName),
    api.getSourceActionRuns(sourceName),
    api.getSourceFiles(sourceName),
  ]);
  const files = loadedFiles.items;
  const nextSelectedFileKey =
    selectedFileKey && files.some((file) => file.file_key === selectedFileKey)
      ? selectedFileKey
      : files[0]?.file_key;
  const loadedFileRuns = nextSelectedFileKey
    ? await api.getSourceFileRuns(sourceName, nextSelectedFileKey)
    : { items: [] };
  return {
    actions: loadedActions.items,
    runs: loadedRuns.items,
    files,
    fileRuns: loadedFileRuns.items,
    selectedFileKey: nextSelectedFileKey,
  };
}

function statusBadgeClass(status: SourceRunStatus | "missing" | "disabled"): string {
  switch (status) {
    case "succeeded":
      return "border-green-200 bg-green-100 text-green-800";
    case "failed":
      return "border-red-200 bg-red-100 text-red-800";
    case "running":
      return "border-blue-200 bg-blue-100 text-blue-800";
    case "missing":
      return "border-amber-200 bg-amber-100 text-amber-800";
    case "disabled":
      return "border-slate-200 bg-slate-100 text-slate-700";
    default:
      return "border-muted bg-muted text-muted-foreground";
  }
}

function actionLabel(action: SourceAction["action"]): string {
  switch (action) {
    case "pull_source":
      return "Download";
    case "import_clickhouse":
      return "Import";
    case "refresh_explorer_cache":
      return "Explorer cache";
  }
}

function formattedDate(value?: string | null | { Time?: string; Valid?: boolean }): string {
  if (!value) return "-";
  if (typeof value === "string") return formatDate(value);
  if (value.Valid === false) return "-";
  return value.Time ? formatDate(value.Time) : "-";
}

function resultValue(run: SourceActionRun): string {
  if (run.status === "failed" && run.error_message) {
    return run.error_message;
  }
  if (run.action === "pull_source") {
    if (Array.isArray(run.result.files)) {
      return `${run.result.files.length.toLocaleString()} files`;
    }
    return String(run.result.records_written ?? "-");
  }
  if (run.action === "refresh_explorer_cache") {
    return `${Number(run.result.rows ?? 0).toLocaleString()} rows`;
  }
  return String(run.result.imported_rows ?? "-");
}

function fileState(file: SourceFileStatus): SourceRunStatus | "missing" | "disabled" {
  if (!file.enabled) return "disabled";
  if (file.latest_status === "running") return "running";
  if (file.missing) return "missing";
  return file.latest_status ?? "missing";
}

function fileRunResult(run: SourceFileRun): string {
  if (run.status === "failed" && run.error_message) return run.error_message;
  if (run.records_written != null) return run.records_written.toLocaleString();
  if (run.content_length_bytes != null) return `${run.content_length_bytes.toLocaleString()} bytes`;
  return "-";
}

function fileImportSupported(file: SourceFileStatus): boolean {
  return file.kind === "source_snapshot" || file.kind === "code_list";
}

function compactLog(value: unknown): string {
  if (value == null) return "-";
  if (Array.isArray(value) && value.length === 0) return "-";
  const text = typeof value === "string" ? value : JSON.stringify(value);
  if (!text || text === "{}" || text === "[]") return "-";
  return text.length > 180 ? `${text.slice(0, 177)}...` : text;
}

function temporalStatusLabel(status?: SourceRunTemporalStatus): string | undefined {
  if (!status) return undefined;
  return status.temporal_status_error ?? status.temporal_status;
}

export function ActionsTab({ source }: ActionsTabProps) {
  const [actions, setActions] = useState<SourceAction[]>([]);
  const [runs, setRuns] = useState<SourceActionRun[]>([]);
  const [files, setFiles] = useState<SourceFileStatus[]>([]);
  const [selectedFileKey, setSelectedFileKey] = useState<string>();
  const [fileRuns, setFileRuns] = useState<SourceFileRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingFileRuns, setLoadingFileRuns] = useState(false);
  const [triggering, setTriggering] = useState<TriggerKey>();
  const [triggeringFileKey, setTriggeringFileKey] = useState<string>();
  const [importingFileKey, setImportingFileKey] = useState<string>();
  const [checkingRunID, setCheckingRunID] = useState<string>();
  const [temporalStatuses, setTemporalStatuses] = useState<Record<string, SourceRunTemporalStatus>>({});
  const [error, setError] = useState<string>();

  async function refresh() {
    const loaded = await loadSourceActionData(source.name, selectedFileKey);
    setActions(loaded.actions);
    setRuns(loaded.runs);
    setFiles(loaded.files);
    setSelectedFileKey(loaded.selectedFileKey);
    setFileRuns(loaded.fileRuns);
  }

  useEffect(() => {
    let ignore = false;
    setLoading(true);
    setError(undefined);
    loadSourceActionData(source.name)
      .then((loaded) => {
        if (!ignore) {
          setActions(loaded.actions);
          setRuns(loaded.runs);
          setFiles(loaded.files);
          setSelectedFileKey(loaded.selectedFileKey);
          setFileRuns(loaded.fileRuns);
        }
      })
      .catch((err) => {
        if (!ignore) {
          setError(errorMessage(err, "Failed to load source actions."));
        }
      })
      .finally(() => {
        if (!ignore) setLoading(false);
      });
    return () => {
      ignore = true;
    };
  }, [source.name]);

  const actionsByKey = useMemo(
    () => new Map(actions.map((action) => [action.action, action])),
    [actions],
  );
  const selectedFile = useMemo(
    () => files.find((file) => file.file_key === selectedFileKey),
    [files, selectedFileKey],
  );
  const downloadEnabled = actionsByKey.get("pull_source")?.enabled ?? false;
  const importEnabled = actionsByKey.get("import_clickhouse")?.enabled ?? false;
  const explorerRefreshEnabled =
    actionsByKey.get("refresh_explorer_cache")?.enabled ?? false;
  const busy = Boolean(triggering || triggeringFileKey || importingFileKey);

  async function runAndRefresh(key: TriggerKey, runner: () => Promise<unknown>) {
    setTriggering(key);
    try {
      await runner();
      toast.success("Workflow started.");
    } catch (err) {
      toast.error(errorMessage(err, "Failed to start workflow."));
      setTriggering(undefined);
      return;
    }
    try {
      await refresh();
    } catch (err) {
      setError(errorMessage(err, "Workflow started, but refresh failed."));
      toast.error(errorMessage(err, "Workflow started, but refresh failed."));
    } finally {
      setTriggering(undefined);
    }
  }

  function triggerDownload() {
    void runAndRefresh("pull_source", () =>
      api.triggerSourceAction(source.name, "pull_source", { trigger: "manual" }),
    );
  }

  function triggerImport() {
    void runAndRefresh("import_clickhouse", () =>
      api.triggerSourceAction(source.name, "import_clickhouse", {
        trigger: "manual",
        batch_size: 1000,
      }),
    );
  }

  function triggerSync() {
    void runAndRefresh("sync", () =>
      api.triggerSourceSyncClickHouse(source.name, {
        trigger: "manual",
        batch_size: 1000,
      }),
    );
  }

  function triggerExplorerRefresh() {
    void runAndRefresh("refresh_explorer_cache", () =>
      api.triggerSourceAction(source.name, "refresh_explorer_cache", {
        trigger: "manual",
      }),
    );
  }

  function triggerRefresh() {
    setLoading(true);
    setError(undefined);
    refresh()
      .catch((err) => {
        setError(errorMessage(err, "Failed to load source actions."));
      })
      .finally(() => setLoading(false));
  }

  function selectFile(fileKey: string) {
    setSelectedFileKey(fileKey);
    setLoadingFileRuns(true);
    api
      .getSourceFileRuns(source.name, fileKey)
      .then((loaded) => setFileRuns(loaded.items))
      .catch((err) => {
        setError(errorMessage(err, "Failed to load source file runs."));
      })
      .finally(() => setLoadingFileRuns(false));
  }

  function triggerFileDownload(file: SourceFileStatus) {
    setTriggeringFileKey(file.file_key);
    api
      .triggerSourceFileDownload(source.name, file.file_key, { trigger: "manual" })
      .then(() => {
        toast.success("File download workflow started.");
        return loadSourceActionData(source.name, file.file_key);
      })
      .then((loaded) => {
        setActions(loaded.actions);
        setRuns(loaded.runs);
        setFiles(loaded.files);
        setSelectedFileKey(loaded.selectedFileKey);
        setFileRuns(loaded.fileRuns);
      })
      .catch((err) => {
        toast.error(errorMessage(err, "Failed to start file download."));
      })
      .finally(() => setTriggeringFileKey(undefined));
  }

  function triggerFileImport(file: SourceFileStatus) {
    setImportingFileKey(file.file_key);
    api
      .triggerSourceFileImport(source.name, file.file_key, {
        trigger: "manual",
        batch_size: 1000,
      })
      .then(() => {
        toast.success("File import workflow started.");
        return loadSourceActionData(source.name, file.file_key);
      })
      .then((loaded) => {
        setActions(loaded.actions);
        setRuns(loaded.runs);
        setFiles(loaded.files);
        setSelectedFileKey(loaded.selectedFileKey);
        setFileRuns(loaded.fileRuns);
      })
      .catch((err) => {
        toast.error(errorMessage(err, "Failed to start file import."));
      })
      .finally(() => setImportingFileKey(undefined));
  }

  function checkTemporalStatus(kind: "action" | "file", runID: string) {
    setCheckingRunID(runID);
    const request =
      kind === "action"
        ? api.getSourceActionRunTemporalStatus(runID)
        : api.getSourceFileRunTemporalStatus(runID);
    request
      .then((status) => {
        setTemporalStatuses((current) => ({ ...current, [runID]: status }));
        toast.success(temporalStatusLabel(status) ?? status.db_status);
      })
      .catch((err) => {
        toast.error(errorMessage(err, "Failed to read Temporal status."));
      })
      .finally(() => setCheckingRunID(undefined));
  }

  if (loading && actions.length === 0 && runs.length === 0 && files.length === 0) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-14 w-full" />
        <Skeleton className="h-48 w-full" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {error ? (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-wrap items-center gap-2 border-b pb-4">
        <Button
          size="sm"
          onClick={triggerDownload}
          disabled={busy || loading || !downloadEnabled}
        >
          <Download className="size-4" />
          Download
        </Button>
        <Button
          size="sm"
          variant="outline"
          onClick={triggerImport}
          disabled={busy || loading || !importEnabled}
        >
          <Upload className="size-4" />
          Import
        </Button>
        <Button
          size="sm"
          variant="outline"
          onClick={triggerSync}
          disabled={busy || loading || !downloadEnabled || !importEnabled}
        >
          <RefreshCw className="size-4" />
          Download and import
        </Button>
        {actionsByKey.has("refresh_explorer_cache") ? (
          <Button
            size="sm"
            variant="outline"
            onClick={triggerExplorerRefresh}
            disabled={busy || loading || !explorerRefreshEnabled}
          >
            <RefreshCw className="size-4" />
            Refresh explorer
          </Button>
        ) : null}
        <Button
          size="sm"
          variant="ghost"
          onClick={triggerRefresh}
          disabled={busy || loading}
          className="ml-auto"
        >
          <RefreshCw className="size-4" />
          Refresh
        </Button>
      </div>

      <section className="space-y-2">
        <h2 className="text-sm font-medium">Configured actions</h2>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Action</TableHead>
              <TableHead>Workflow</TableHead>
              <TableHead>Task queue</TableHead>
              <TableHead>Status</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {actions.map((action) => (
              <TableRow key={action.id}>
                <TableCell>
                  <div className="font-medium">{action.display_name}</div>
                  <div className="font-mono text-xs text-muted-foreground">
                    {action.action}
                  </div>
                </TableCell>
                <TableCell className="font-mono text-xs">
                  {action.temporal_workflow_type}
                </TableCell>
                <TableCell className="font-mono text-xs">
                  {action.temporal_task_queue ?? "-"}
                </TableCell>
                <TableCell>
                  <Badge variant="outline">
                    {action.enabled ? "Enabled" : "Disabled"}
                  </Badge>
                </TableCell>
              </TableRow>
            ))}
            {actions.length === 0 ? (
              <TableRow>
                <TableCell colSpan={4} className="h-16 text-muted-foreground">
                  No actions configured.
                </TableCell>
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </section>

      <section className="space-y-2">
        <div className="flex items-center justify-between gap-3">
          <h2 className="text-sm font-medium">Source files</h2>
          <div className="text-xs text-muted-foreground">
            {files.length.toLocaleString()} configured
          </div>
        </div>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>File</TableHead>
              <TableHead>Required</TableHead>
              <TableHead>State</TableHead>
              <TableHead>Latest run</TableHead>
              <TableHead>Latest successful path</TableHead>
              <TableHead>Records</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {files.map((file) => {
              const state = fileState(file);
              const isSelected = file.file_key === selectedFileKey;
              const canImportFile =
                importEnabled &&
                file.enabled &&
                fileImportSupported(file) &&
                Boolean(file.latest_successful_run_id);
              return (
                <TableRow
                  key={file.id}
                  className={cn(isSelected && "bg-muted/40")}
                >
                  <TableCell>
                    <div className="font-medium">{file.display_name}</div>
                    <div className="font-mono text-xs text-muted-foreground">
                      {file.file_key}
                    </div>
                    <div className="max-w-80 truncate font-mono text-xs text-muted-foreground">
                      {file.relative_path}
                    </div>
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline">{file.required ? "Required" : "Optional"}</Badge>
                  </TableCell>
                  <TableCell>
                    <Badge className={statusBadgeClass(state)} variant="outline">
                      {state}
                    </Badge>
                    {file.latest_error_message ? (
                      <div className="mt-1 max-w-72 whitespace-normal text-xs text-red-700">
                        {file.latest_error_message}
                      </div>
                    ) : null}
                  </TableCell>
                  <TableCell>
                    <div>{formattedDate(file.latest_started_at)}</div>
                    <div className="text-xs text-muted-foreground">
                      {formattedDate(file.latest_finished_at)}
                    </div>
                  </TableCell>
                  <TableCell className="max-w-80 truncate font-mono text-xs">
                    {file.latest_successful_path ?? "-"}
                  </TableCell>
                  <TableCell>
                    {file.latest_records_written != null
                      ? file.latest_records_written.toLocaleString()
                      : "-"}
                  </TableCell>
                  <TableCell>
                    <div className="flex justify-end gap-2">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => selectFile(file.file_key)}
                        disabled={loadingFileRuns && isSelected}
                      >
                        <Search className="size-4" />
                        Runs
                      </Button>
                      <Button
                        size="sm"
                        onClick={() => triggerFileDownload(file)}
                        disabled={busy || loading || !downloadEnabled || !file.enabled}
                      >
                        <FileDown className="size-4" />
                        {triggeringFileKey === file.file_key ? "Starting" : "Download"}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => triggerFileImport(file)}
                        disabled={busy || loading || !canImportFile}
                        title={
                          fileImportSupported(file)
                            ? "Import latest successful file run"
                            : "This source importer does not import this file kind yet"
                        }
                      >
                        <Upload className="size-4" />
                        {importingFileKey === file.file_key ? "Starting" : "Import"}
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              );
            })}
            {files.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} className="h-16 text-muted-foreground">
                  No source files configured.
                </TableCell>
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </section>

      <section className="space-y-2">
        <div className="flex items-center justify-between gap-3">
          <h2 className="text-sm font-medium">
            File runs{selectedFile ? `: ${selectedFile.display_name}` : ""}
          </h2>
          {selectedFile ? (
            <div className="font-mono text-xs text-muted-foreground">
              {selectedFile.file_key}
            </div>
          ) : null}
        </div>
        {loadingFileRuns ? (
          <Skeleton className="h-32 w-full" />
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Status</TableHead>
                <TableHead>Started</TableHead>
                <TableHead>Finished</TableHead>
                <TableHead>Path</TableHead>
                <TableHead>Result</TableHead>
                <TableHead>Workflow</TableHead>
                <TableHead>Log</TableHead>
                <TableHead className="text-right">Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {fileRuns.map((run) => {
                const temporalStatus = temporalStatusLabel(temporalStatuses[run.id]);
                return (
                  <TableRow key={run.id}>
                    <TableCell>
                      <Badge className={statusBadgeClass(run.status)} variant="outline">
                        {run.status}
                      </Badge>
                      {temporalStatus ? (
                        <div className="mt-1 text-xs text-muted-foreground">
                          {temporalStatus}
                        </div>
                      ) : null}
                    </TableCell>
                    <TableCell>{formattedDate(run.started_at)}</TableCell>
                    <TableCell>{formattedDate(run.finished_at)}</TableCell>
                    <TableCell className="max-w-72 truncate font-mono text-xs">
                      {run.path ?? "-"}
                    </TableCell>
                    <TableCell className="max-w-72 whitespace-normal font-mono text-xs">
                      {fileRunResult(run)}
                    </TableCell>
                    <TableCell className="max-w-64 truncate font-mono text-xs">
                      {run.temporal_workflow_id ?? "-"}
                    </TableCell>
                    <TableCell className="max-w-80 whitespace-normal font-mono text-xs">
                      {compactLog(run.error_message ?? run.log)}
                    </TableCell>
                    <TableCell>
                      <div className="flex justify-end">
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() => checkTemporalStatus("file", run.id)}
                          disabled={checkingRunID === run.id}
                          aria-label="Check file run Temporal status"
                          title="Check Temporal status"
                        >
                          <RefreshCw className="size-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}
              {fileRuns.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="h-16 text-muted-foreground">
                    No file runs found.
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        )}
      </section>

      <section className="space-y-2">
        <h2 className="text-sm font-medium">Action runs</h2>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Action</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Started</TableHead>
              <TableHead>Finished</TableHead>
              <TableHead>Workflow</TableHead>
              <TableHead>Result</TableHead>
              <TableHead className="text-right">Status</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {runs.map((run) => {
              const temporalStatus = temporalStatusLabel(temporalStatuses[run.id]);
              return (
                <TableRow key={run.id}>
                  <TableCell>
                    <div className="font-medium">{actionLabel(run.action)}</div>
                    <div className="font-mono text-xs text-muted-foreground">
                      {run.id}
                    </div>
                  </TableCell>
                  <TableCell>
                    <Badge className={statusBadgeClass(run.status)} variant="outline">
                      {run.status}
                    </Badge>
                    {temporalStatus ? (
                      <div className="mt-1 text-xs text-muted-foreground">
                        {temporalStatus}
                      </div>
                    ) : null}
                  </TableCell>
                  <TableCell>{formattedDate(run.started_at)}</TableCell>
                  <TableCell>{formattedDate(run.finished_at)}</TableCell>
                  <TableCell className="max-w-64 truncate font-mono text-xs">
                    {run.temporal_workflow_id ?? "-"}
                  </TableCell>
                  <TableCell className="max-w-80 whitespace-normal font-mono text-xs">
                    {resultValue(run)}
                  </TableCell>
                  <TableCell>
                    <div className="flex justify-end">
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => checkTemporalStatus("action", run.id)}
                        disabled={checkingRunID === run.id}
                        aria-label="Check action run Temporal status"
                        title="Check Temporal status"
                      >
                        <RefreshCw className="size-4" />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              );
            })}
            {runs.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} className="h-16 text-muted-foreground">
                  No runs found.
                </TableCell>
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </section>
    </div>
  );
}
