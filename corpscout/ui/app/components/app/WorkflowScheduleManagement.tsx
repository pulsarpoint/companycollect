import { useEffect, useMemo, useState } from "react";
import {
  Pause,
  Play,
  Plus,
  RefreshCw,
  Save,
  Trash2,
} from "lucide-react";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { api, errorMessage } from "~/lib/api";
import type {
  WorkflowSchedule,
  WorkflowScheduleInput,
  WorkflowScheduleSpec,
} from "~/types/api";
import { ScheduleEditor } from "~/components/app/ScheduleEditor";
import {
  NACETaxonomySyncForm,
  defaultNACETaxonomySyncFormValue,
  type NACETaxonomySyncFormValue,
} from "~/components/app/NACETaxonomySyncForm";

type ScheduleForm = {
  scheduleID: string;
  displayName: string;
  description: string;
};

const defaultScheduleSpec: WorkflowScheduleSpec = {
  timezone: "Europe/Belgrade",
  cron_expression: "0 3 * * *",
  overlap_policy: "skip",
  catchup_window_seconds: 3600,
};

const emptyForm: ScheduleForm = {
  scheduleID: "nace-taxonomy-sync-nightly",
  displayName: "Nightly NACE taxonomy sync",
  description: "",
};

export function WorkflowScheduleManagement() {
  const [schedules, setSchedules] = useState<WorkflowSchedule[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [sheetOpen, setSheetOpen] = useState(false);
  const [selectedID, setSelectedID] = useState("");
  const [form, setForm] = useState<ScheduleForm>(emptyForm);
  const [enabled, setEnabled] = useState(true);
  const [spec, setSpec] = useState<WorkflowScheduleSpec>(defaultScheduleSpec);
  const [naceInput, setNaceInput] =
    useState<NACETaxonomySyncFormValue>(defaultNACETaxonomySyncFormValue);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);

  const selectedSchedule = useMemo(
    () =>
      schedules.find(
        (schedule) => schedule.temporal_schedule_id === selectedID,
      ) ?? null,
    [schedules, selectedID],
  );

  useEffect(() => {
    void loadSchedules();
  }, []);

  async function loadSchedules() {
    setLoading(true);
    setError(null);
    try {
      const response = await api.getWorkflowSchedules();
      setSchedules(response.items);
    } catch (err) {
      setError(errorMessage(err, "Failed to load workflow schedules"));
    } finally {
      setLoading(false);
    }
  }

  function openCreateSheet() {
    setSelectedID("");
    setForm(emptyForm);
    setEnabled(true);
    setSpec(defaultScheduleSpec);
    setNaceInput(defaultNACETaxonomySyncFormValue);
    setMessage(null);
    setError(null);
    setSheetOpen(true);
  }

  function openEditSheet(schedule: WorkflowSchedule) {
    setSelectedID(schedule.temporal_schedule_id);
    setForm({
      scheduleID: schedule.temporal_schedule_id,
      displayName: schedule.display_name,
      description: schedule.description ?? "",
    });
    setEnabled(schedule.enabled);
    setSpec({
      timezone: schedule.spec.timezone || defaultScheduleSpec.timezone,
      cron_expression:
        schedule.spec.cron_expression || defaultScheduleSpec.cron_expression,
      overlap_policy:
        schedule.spec.overlap_policy || defaultScheduleSpec.overlap_policy,
      catchup_window_seconds:
        schedule.spec.catchup_window_seconds ??
        defaultScheduleSpec.catchup_window_seconds,
    });
    setNaceInput({
      revision: stringFromRecord(schedule.action_input, "revision") || "2.1",
      source_url: stringFromRecord(schedule.action_input, "source_url"),
      force_reprocess: booleanFromRecord(
        schedule.action_input,
        "force_reprocess",
      ),
    });
    setMessage(null);
    setError(null);
    setSheetOpen(true);
  }

  async function saveSchedule() {
    setSaving(true);
    setError(null);
    setMessage(null);
    try {
      const body: WorkflowScheduleInput = {
        temporal_schedule_id: form.scheduleID.trim(),
        workflow_key: "nace_taxonomy_sync",
        display_name: form.displayName.trim(),
        description: form.description.trim(),
        enabled,
        tags: ["taxonomy", "nace"],
        metadata: {},
        spec,
        action_input: {
          revision: naceInput.revision.trim(),
          source_url: naceInput.source_url.trim(),
          trigger: "schedule",
          force_reprocess: naceInput.force_reprocess,
        },
      };
      if (selectedSchedule) {
        await api.updateWorkflowSchedule(selectedSchedule.temporal_schedule_id, body);
        setMessage("Schedule updated");
      } else {
        await api.createWorkflowSchedule(body);
        setMessage("Schedule created");
      }
      await loadSchedules();
      setSheetOpen(false);
    } catch (err) {
      setError(errorMessage(err, "Failed to save workflow schedule"));
    } finally {
      setSaving(false);
    }
  }

  async function triggerSchedule(schedule: WorkflowSchedule) {
    await scheduleAction(
      () => api.triggerWorkflowSchedule(schedule.temporal_schedule_id),
      "Schedule triggered",
    );
  }

  async function pauseSchedule(schedule: WorkflowSchedule) {
    await scheduleAction(
      () =>
        api.pauseWorkflowSchedule(
          schedule.temporal_schedule_id,
          "Paused from Corpscout",
        ),
      "Schedule paused",
    );
  }

  async function resumeSchedule(schedule: WorkflowSchedule) {
    await scheduleAction(
      () =>
        api.resumeWorkflowSchedule(
          schedule.temporal_schedule_id,
          "Resumed from Corpscout",
        ),
      "Schedule resumed",
    );
  }

  async function deleteSchedule(schedule: WorkflowSchedule) {
    await scheduleAction(
      () => api.deleteWorkflowSchedule(schedule.temporal_schedule_id),
      "Schedule deleted",
    );
  }

  async function scheduleAction(
    action: () => Promise<unknown>,
    successMessage: string,
  ) {
    setError(null);
    setMessage(null);
    try {
      await action();
      setMessage(successMessage);
      await loadSchedules();
    } catch (err) {
      setError(errorMessage(err, "Workflow schedule action failed"));
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-xl font-semibold">Workflow Schedules</h1>
          <p className="text-sm text-muted-foreground">
            Temporal schedules managed from Corpscout.
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => void loadSchedules()}>
            <RefreshCw className="size-4" />
            Refresh
          </Button>
          <Button onClick={openCreateSheet}>
            <Plus className="size-4" />
            New schedule
          </Button>
        </div>
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      {message && (
        <Alert>
          <AlertDescription>{message}</AlertDescription>
        </Alert>
      )}

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Workflow</TableHead>
              <TableHead>Cron</TableHead>
              <TableHead>State</TableHead>
              <TableHead>Next run</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <TableRow>
                <TableCell colSpan={6} className="py-8 text-center text-sm text-muted-foreground">
                  Loading schedules
                </TableCell>
              </TableRow>
            ) : schedules.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6} className="py-8 text-center text-sm text-muted-foreground">
                  No workflow schedules
                </TableCell>
              </TableRow>
            ) : (
              schedules.map((schedule) => (
                <TableRow key={schedule.temporal_schedule_id}>
                  <TableCell>
                    <button
                      type="button"
                      className="text-left font-medium hover:underline"
                      onClick={() => openEditSheet(schedule)}
                    >
                      {schedule.display_name}
                    </button>
                    <div className="text-xs text-muted-foreground">
                      {schedule.temporal_schedule_id}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="font-mono text-xs">{schedule.workflow_name}</div>
                    <div className="text-xs text-muted-foreground">
                      {schedule.task_queue}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="font-mono text-xs">
                      {schedule.spec.cron_expression || "-"}
                    </div>
                    <div className="text-xs text-muted-foreground">
                      {schedule.spec.timezone}
                    </div>
                  </TableCell>
                  <TableCell>
                    <ScheduleStateBadge schedule={schedule} />
                  </TableCell>
                  <TableCell className="text-sm">
                    {schedule.temporal.next_run_at
                      ? new Date(schedule.temporal.next_run_at).toLocaleString()
                      : "-"}
                  </TableCell>
                  <TableCell>
                    <div className="flex justify-end gap-1">
                      <Button
                        size="icon"
                        variant="ghost"
                        title="Trigger now"
                        onClick={() => void triggerSchedule(schedule)}
                      >
                        <Play className="size-4" />
                      </Button>
                      {schedule.temporal.paused ? (
                        <Button
                          size="icon"
                          variant="ghost"
                          title="Resume"
                          onClick={() => void resumeSchedule(schedule)}
                        >
                          <RefreshCw className="size-4" />
                        </Button>
                      ) : (
                        <Button
                          size="icon"
                          variant="ghost"
                          title="Pause"
                          onClick={() => void pauseSchedule(schedule)}
                        >
                          <Pause className="size-4" />
                        </Button>
                      )}
                      <Button
                        size="icon"
                        variant="ghost"
                        title="Delete"
                        onClick={() => void deleteSchedule(schedule)}
                      >
                        <Trash2 className="size-4" />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      <Sheet open={sheetOpen} onOpenChange={setSheetOpen}>
        <SheetContent className="w-full overflow-y-auto sm:max-w-2xl">
          <SheetHeader>
            <SheetTitle>
              {selectedSchedule ? "Edit schedule" : "Create schedule"}
            </SheetTitle>
            <SheetDescription>
              NACE taxonomy sync is the first schedulable workflow.
            </SheetDescription>
          </SheetHeader>

          <div className="mt-5 flex flex-col gap-5">
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="grid gap-2">
                <Label htmlFor="schedule-id">Schedule ID</Label>
                <Input
                  id="schedule-id"
                  value={form.scheduleID}
                  readOnly={Boolean(selectedSchedule)}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      scheduleID: event.target.value,
                    }))
                  }
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="schedule-name">Display name</Label>
                <Input
                  id="schedule-name"
                  value={form.displayName}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      displayName: event.target.value,
                    }))
                  }
                />
              </div>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="schedule-description">Description</Label>
              <Input
                id="schedule-description"
                value={form.description}
                onChange={(event) =>
                  setForm((current) => ({
                    ...current,
                    description: event.target.value,
                  }))
                }
              />
            </div>

            <Separator />

            <ScheduleEditor
              spec={spec}
              enabled={enabled}
              onSpecChange={setSpec}
              onEnabledChange={setEnabled}
            />

            <Separator />

            <NACETaxonomySyncForm value={naceInput} onChange={setNaceInput} />

            <div className="flex justify-end gap-2">
              <Button variant="outline" onClick={() => setSheetOpen(false)}>
                Cancel
              </Button>
              <Button onClick={() => void saveSchedule()} disabled={saving}>
                <Save className="size-4" />
                {saving ? "Saving..." : "Save"}
              </Button>
            </div>
          </div>
        </SheetContent>
      </Sheet>
    </div>
  );
}

function ScheduleStateBadge({ schedule }: { schedule: WorkflowSchedule }) {
  if (!schedule.temporal.exists) {
    return <Badge variant="destructive">Missing</Badge>;
  }
  if (schedule.temporal.paused) {
    return <Badge variant="secondary">Paused</Badge>;
  }
  if (!schedule.enabled) {
    return <Badge variant="secondary">Disabled</Badge>;
  }
  return <Badge>Active</Badge>;
}

function stringFromRecord(record: Record<string, unknown>, key: string): string {
  const value = record[key];
  return typeof value === "string" ? value : "";
}

function booleanFromRecord(
  record: Record<string, unknown>,
  key: string,
): boolean {
  return record[key] === true;
}
