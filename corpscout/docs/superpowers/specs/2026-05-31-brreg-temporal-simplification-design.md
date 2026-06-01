# BRREG Temporal Simplification Design

## Summary

Simplify BRREG action execution by making Temporal own workflow preparation, task selection, batch execution, and final audit updates. Corpscout HTTP handlers and BRREG service code should only validate requests and start concrete Temporal workflows with concrete inputs.

This removes the current indirection where `tasksvc` creates BRREG workflow audit rows, creates task selections, applies BRREG-specific runtime defaults, builds generic starter structs, and then starts Temporal. That logic belongs with the BRREG workflow execution package and the BRREG database gateway.

## Goals

- Make starting a BRREG action easy to read from one function.
- Move BRREG workflow run creation and task selection into Temporal activities.
- Keep Temporal workflows explicit: prepare, run work, finish.
- Keep business state in `brreg_workflow` tables, not in Temporal history.
- Support multi-day domain discovery with bounded parallel one-company activities and per-company progress.
- Keep translation and financial conversion batch-based because those actions are comparatively uniform.
- Make action activities testable as plain Go functions without needing to test Temporal itself.

## Non-Goals

- Do not redesign the `brreg_workflow` database schema in this refactor.
- Do not change user-facing BRREG raw-record behavior.
- Do not remove workflow/task-status tables.
- Do not replace Temporal.
- Do not move translation, crawl, or financial service implementation into Corpscout.

## Current Problem

The current start path has too many layers:

```text
HTTP handler
  -> BRREG service normalizes request
  -> tasksvc action starter builds env defaults and memo data
  -> tasksvc creates brreg_workflow.workflow_runs
  -> tasksvc creates brreg_workflow.task_selections
  -> tasksvc starts Temporal workflow with workflow_run_id and selection_hash
  -> workflow loops ExecuteActivity(batch)
```

This makes functions such as `StartBrregDomainDiscovery` hard to understand. Opening the starter does not show the real operation; the reader must chase generic structs, selection helpers, memo helpers, runtime default helpers, and DB cleanup paths.

## Target Architecture

```text
HTTP handler
  -> BRREG service
  -> tasksvc starts concrete Temporal workflow

Temporal workflow
  -> prepare workflow activity
  -> work activities
  -> finish workflow activity

BRREG DB gateway
  -> create workflow run
  -> create task selection
  -> claim task records
  -> submit result/failure
  -> finish workflow run
```

`tasksvc` should not know how BRREG task selections are stored. It should not know selection hashes, workflow run IDs from Corpscout tables, or BRREG workflow audit cleanup. It should know only how to call `TemporalClient.ExecuteWorkflow` with a workflow name, workflow ID, memo/search attributes, and a concrete input.

## Start Path

Each BRREG starter should be explicit and concrete:

```text
StartBrregTranslation
  -> build BrregTranslateWorkflowInput from request and environment defaults
  -> ExecuteWorkflow(TranslateBrregRawInputs, input)

StartBrregFinancialConversion
  -> build BrregConvertFinancialsWorkflowInput from request and environment defaults
  -> ExecuteWorkflow(ConvertBrregFinancials, input)

StartBrregDomainDiscovery
  -> build BrregDomainWorkflowInput from request and environment defaults
  -> ExecuteWorkflow(DiscoverBrregDomains, input)
```

The input should carry request scope directly:

- `trigger`
- `ids`
- `filters`
- `limit`
- `batch_size`
- `max_task_attempts`
- action-specific fields such as `fx_rate_date` or `search_provider`
- runtime controls such as `max_parallel_tasks`, `lease_seconds`, and `continue_as_new_after_batches`

Workflow IDs remain singleton for full/current-record runs and unique for selected runs.

## Workflow Preparation

Each top-level workflow starts with a prepare activity:

```text
PrepareBrregTranslationWorkflow
PrepareBrregFinancialWorkflow
PrepareBrregDomainWorkflow
```

The prepare activity:

- normalizes IDs and filters
- resolves default limit, batch size, and max task attempts
- creates `brreg_workflow.workflow_runs`
- creates `brreg_workflow.task_selections`
- returns `workflow_run_id`, `selection_hash`, selected count, resolved batch size, and resolved max attempts

It is acceptable that the HTTP endpoint returns a Temporal workflow/run ID before the Corpscout `brreg_workflow.workflow_runs` row exists. The row appears when the first workflow activity runs.

## Translation Workflow

Translation remains a batch workflow:

```text
TranslateBrregRawInputs
  -> PrepareBrregTranslationWorkflow
  -> loop TranslateNextBrregBatch until no rows
  -> FinishBrregWorkflowRun
```

`TranslateNextBrregBatch`:

- claims a selected batch
- calls translation service
- writes translation result rows
- writes retryable or terminal failures
- returns counters

Batch size and max parallel task claims are configured per workflow run.

## Financial Workflow

Financial conversion remains a batch workflow:

```text
ConvertBrregFinancials
  -> PrepareBrregFinancialWorkflow
  -> loop ConvertNextBrregFinancialBatch until no rows
  -> FinishBrregWorkflowRun
```

`ConvertNextBrregFinancialBatch`:

- claims a selected batch
- loads FX rates
- converts financial amounts to USD
- writes financial result rows
- writes retryable or terminal failures
- returns counters

## Domain Discovery Workflow

Domain discovery is different because it can take multiple days. It should not process large groups inside one long activity.

Use one parent workflow that runs bounded parallel one-company activities:

```text
DiscoverBrregDomains parent workflow
  -> PrepareBrregDomainWorkflow
  -> claim a page of pending selected companies
  -> run DiscoverOneBrregDomainCompany activities up to max_parallel_company_activities
  -> wait for activity completions
  -> repeat until no rows remain
  -> continue-as-new periodically
  -> FinishBrregWorkflowRun

DiscoverOneBrregDomainCompany activity
  -> call crawl-service once for one company
  -> submit domain result/failure
```

The parent workflow coordinates company-level concurrency. Each activity owns one company request to the external crawl/domain service and commits that company result independently.

This avoids modeling crawl-service internals twice. Temporal does not need a child workflow unless Corpscout later splits one-company domain discovery into multiple Temporal-visible steps such as search, crawl, LLM verification, human review, or delayed follow-up.

## Domain Concurrency Controls

Domain discovery must have explicit limits at two layers.

Workflow-level controls:

- `max_parallel_company_activities`: maximum active one-company domain activities for one parent run
- `max_claim_page_size`: maximum number of companies claimed from the DB at once
- `continue_as_new_after_companies`: maximum completed companies before parent continues as new

Worker-level controls:

- `MaxConcurrentWorkflowTaskExecutionSize`
- `MaxConcurrentActivityExecutionSize`
- `MaxConcurrentLocalActivityExecutionSize` if local activities are used

The workflow-level limits protect external services and make business behavior predictable. Worker-level limits protect the worker process and the host.

## Database Gateway Responsibilities

BRREG database methods should hide transaction details:

```text
PrepareTranslationWorkflow(command) -> PreparedWorkflow
PrepareFinancialWorkflow(command) -> PreparedWorkflow
PrepareDomainWorkflow(command) -> PreparedWorkflow

ClaimTranslationBatch(command) -> []ClaimedRawRecord
ClaimFinancialBatch(command) -> []ClaimedRawRecord
ClaimDomainCompanyPage(command) -> []ClaimedRawRecord

SubmitTranslationResult(command)
SubmitFinancialResult(command)
SubmitDomainResult(command)
SubmitTaskFailure(command)
FinishWorkflowRun(command)
```

Each submit method writes artifact rows and task state in one transaction. Claim methods enforce selection, retry policy, stale leases, and max active claims.

## Observability

Temporal remains responsible for durable execution history:

- workflow started
- prepare activity executed
- batch or one-company activity progress
- retries
- cancellation
- completion or failure

`brreg_workflow` tables remain responsible for product state:

- which records were selected
- which records are pending/running/succeeded/failed
- failure category and retry strategy
- artifact rows for translation, financial conversion, and domains

The UI should continue reading live BRREG workflow views for record/task state rather than relying on Temporal history as the source of product truth.

## Error Handling

HTTP start errors:

- invalid request returns `400`
- already-running singleton workflow returns `409`
- unexpected Temporal start failure logs once and returns `500`

Workflow preparation errors:

- fail the Temporal workflow if the DB selection cannot be created
- no separate HTTP cleanup path is needed because preparation is inside Temporal

Batch and one-company activity errors:

- retry transient external service failures with Temporal retry/backoff
- record retryable failures in `brreg_workflow.raw_record_task_states`
- record terminal failures when max attempts are exhausted or output is invalid
- keep committed per-record progress even if later records fail

## Testing Strategy

HTTP and service tests:

- request body maps to BRREG action command
- action command maps to concrete Temporal input
- already-started errors return conflict responses

Task starter tests:

- each starter calls `ExecuteWorkflow` with the expected concrete workflow name and input
- singleton and selected workflow IDs are generated correctly
- BRREG DB selection methods are not called by tasksvc

Workflow tests:

- workflow executes prepare activity before work activity
- translation and financial workflows drain batches until no rows remain
- domain parent respects `max_parallel_company_activities`
- domain parent continues as new after configured completed company count
- finish activity is called with counters

Activity tests:

- prepare activities create workflow run and task selection
- batch activities claim rows, call the external service client, and submit results
- domain one-company activities submit one company result/failure independently
- retryable and terminal failures update task state correctly

## Refactor Sequence

1. Add concrete workflow input fields for selection request data.
2. Add prepare activities and gateway methods for translation, financial conversion, and domain discovery.
3. Move task selection creation from `tasksvc` into prepare activities.
4. Simplify `tasksvc` BRREG starters to direct `ExecuteWorkflow` calls.
5. Keep translation and financial workflows batch-based.
6. Refactor domain discovery into a parent workflow with bounded parallel one-company activities.
7. Remove obsolete BRREG generic starter helpers and tests that enforce the old indirection.
8. Verify UI task state still reads from `brreg_workflow` views.
