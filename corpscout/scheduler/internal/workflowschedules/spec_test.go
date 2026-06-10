package workflowschedules

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"

	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
	naceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/nace"
)

func TestWorkflowDefinitionsIncludeNACETaxonomySync(t *testing.T) {
	def, ok := DefinitionByKey("nace_taxonomy_sync")
	require.True(t, ok)
	require.Equal(t, "nace_taxonomy_sync", def.Key)
	require.Equal(t, naceworkflow.SyncWorkflowName, def.WorkflowName)
	require.Equal(t, naceworkflow.SyncTaskQueue, def.TaskQueue)
	require.Equal(t, "taxonomy", def.Domain)
	require.Equal(t, "nace_taxonomy_sync", def.Purpose)
}

func TestBuildNACEScheduleActionInput(t *testing.T) {
	def, ok := DefinitionByKey("nace_taxonomy_sync")
	require.True(t, ok)

	input, err := def.DecodeActionInput(json.RawMessage(`{
		"revision": "NACE Rev. 2.1",
		"source_url": "https://example.test/nace.rdf",
		"trigger": "schedule",
		"force_reprocess": true
	}`))
	require.NoError(t, err)

	typed, ok := input.(naceworkflow.SyncNACETaxonomyInput)
	require.True(t, ok)
	require.Equal(t, "NACE Rev. 2.1", typed.Revision)
	require.Equal(t, "https://example.test/nace.rdf", typed.SourceURL)
	require.Equal(t, "schedule", typed.Trigger)
	require.True(t, typed.ForceReprocess)
}

func TestBuildNACEScheduleActionInputDefaults(t *testing.T) {
	def, ok := DefinitionByKey("nace_taxonomy_sync")
	require.True(t, ok)

	input, err := def.DecodeActionInput(json.RawMessage(`{"source_url":"https://example.test/nace.rdf"}`))
	require.NoError(t, err)

	typed, ok := input.(naceworkflow.SyncNACETaxonomyInput)
	require.True(t, ok)
	require.Equal(t, nacetaxonomy.DefaultRevision, typed.Revision)
	require.Equal(t, "schedule", typed.Trigger)
	require.Equal(t, "https://example.test/nace.rdf", typed.SourceURL)
}

func TestBuildNACEScheduleActionInputRequiresSourceURL(t *testing.T) {
	def, ok := DefinitionByKey("nace_taxonomy_sync")
	require.True(t, ok)

	_, err := def.DecodeActionInput(nil)
	require.ErrorContains(t, err, "nace source url is required")
}

func TestBuildScheduleSpecValidatesCron(t *testing.T) {
	spec, err := BuildScheduleSpec(ScheduleSpecInput{
		CronExpression:       "0 3 * * *",
		Timezone:             "Europe/Belgrade",
		OverlapPolicy:        "skip",
		CatchupWindowSeconds: 3600,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"0 3 * * *"}, spec.CronExpressions)
	require.Equal(t, "Europe/Belgrade", spec.TimeZoneName)
}

func TestBuildScheduleSpecRejectsInvalidCron(t *testing.T) {
	_, err := BuildScheduleSpec(ScheduleSpecInput{
		CronExpression: "not enough fields",
		Timezone:       "Europe/Belgrade",
		OverlapPolicy:  "skip",
	})
	require.ErrorContains(t, err, "cron expression must contain 5 fields")
}

func TestBuildScheduleSpecRejectsInvalidCronSyntax(t *testing.T) {
	_, err := BuildScheduleSpec(ScheduleSpecInput{
		CronExpression: "foo bar baz qux quux",
		Timezone:       "Europe/Belgrade",
		OverlapPolicy:  "skip",
	})
	require.ErrorContains(t, err, "cron expression is invalid")
}

func TestBuildScheduleSpecRejectsInvalidTimezone(t *testing.T) {
	_, err := BuildScheduleSpec(ScheduleSpecInput{
		CronExpression: "0 3 * * *",
		Timezone:       "Mars/Olympus",
		OverlapPolicy:  "skip",
	})
	require.ErrorContains(t, err, "timezone is invalid")
}

func TestBuildScheduleSpecRejectsNegativeCatchupWindow(t *testing.T) {
	_, err := BuildScheduleSpec(ScheduleSpecInput{
		CronExpression:       "0 3 * * *",
		Timezone:             "Europe/Belgrade",
		OverlapPolicy:        "skip",
		CatchupWindowSeconds: -1,
	})
	require.ErrorContains(t, err, "catchup window seconds cannot be negative")
}

func TestOverlapPolicyMapping(t *testing.T) {
	tests := map[string]enumspb.ScheduleOverlapPolicy{
		"skip":            enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		"buffer_one":      enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		"allow_all":       enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		"cancel_other":    enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		"terminate_other": enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
	}
	for value, expected := range tests {
		actual, err := OverlapPolicy(value)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
}

func TestOverlapPolicyRejectsUnsupportedValue(t *testing.T) {
	_, err := OverlapPolicy("buffer_all")
	require.ErrorContains(t, err, "unsupported overlap policy")
}
