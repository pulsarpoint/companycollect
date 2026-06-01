package brregdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareWorkflowDefinitionIsDeterministic(t *testing.T) {
	command := PrepareWorkflowCommand{
		Source:             "brreg",
		Action:             "translate",
		TaskType:           TaskTypeTranslate,
		Trigger:            "manual",
		WorkflowID:         "brreg-translation-test",
		IDs:                []string{"b", "a"},
		Filters:            map[string]string{"state": "raw", "query": "acme"},
		Limit:              0,
		BatchSize:          0,
		MaxAttempts:        0,
		DefaultLimit:       1000,
		DefaultBatchSize:   50,
		DefaultMaxAttempts: 3,
	}

	first, firstHash, err := workflowSelectionDefinition(command)
	require.NoError(t, err)
	second, secondHash, err := workflowSelectionDefinition(command)
	require.NoError(t, err)

	require.JSONEq(t, string(first), string(second))
	require.Equal(t, firstHash, secondHash)
	require.Contains(t, string(first), `"limit":1000`)
	require.Contains(t, string(first), `"batch_size":50`)
	require.Contains(t, string(first), `"max_attempts":3`)
	require.Contains(t, string(first), `"ids":["a","b"]`)
}

func TestStringFilterSupportsUIAliasKeys(t *testing.T) {
	filters := map[string]string{
		"q":               "BORTIGARD",
		"lifecycle_state": "input",
	}

	require.Equal(t, "BORTIGARD", *stringFilter(filters, "query", "q"))
	require.Equal(t, "input", *stringFilter(filters, "state", "lifecycle_state"))
	require.Nil(t, stringFilter(filters, "translation_status"))
}
