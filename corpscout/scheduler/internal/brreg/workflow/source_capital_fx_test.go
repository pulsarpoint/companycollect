package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestConvertBrregSourceCapitalToUSDRunsSingleActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(ConvertBrregSourceCapitalToUSD)
	var activityInput ConvertBrregSourceCapitalToUSDActivityInput
	env.RegisterActivityWithOptions(func(input ConvertBrregSourceCapitalToUSDActivityInput) (ConvertBrregSourceCapitalToUSDActivityResult, error) {
		activityInput = input
		return ConvertBrregSourceCapitalToUSDActivityResult{
			CapitalSeen:                    3,
			CapitalConverted:               2,
			CapitalSkippedMissingRate:      1,
			CapitalSkippedAlreadyConverted: 0,
			RateDate:                       "2026-06-03",
		}, nil
	}, activity.RegisterOptions{Name: convertBrregSourceCapitalToUSDActivity})

	env.ExecuteWorkflow(ConvertBrregSourceCapitalToUSD, ConvertBrregSourceCapitalToUSDInput{
		IDs:            []string{"11111111-1111-1111-1111-111111111111"},
		Filters:        map[string]string{"query": "BORTIGARD"},
		Limit:          3,
		RateDate:       "2026-06-03",
		ForceReprocess: true,
		Trigger:        "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"11111111-1111-1111-1111-111111111111"}, activityInput.IDs)
	require.Equal(t, map[string]string{"query": "BORTIGARD"}, activityInput.Filters)
	require.EqualValues(t, 3, activityInput.Limit)
	require.Equal(t, "2026-06-03", activityInput.RateDate)
	require.True(t, activityInput.ForceReprocess)
	require.Equal(t, "manual", activityInput.Trigger)

	var result ConvertBrregSourceCapitalToUSDResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 3, result.CapitalSeen)
	require.EqualValues(t, 2, result.CapitalConverted)
	require.EqualValues(t, 1, result.CapitalSkippedMissingRate)
	require.Equal(t, "2026-06-03", result.RateDate)
}
