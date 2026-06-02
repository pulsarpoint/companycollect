package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestNormalizeBrregSourceProfilesRunsSingleActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(NormalizeBrregSourceProfiles)
	var activityInput NormalizeBrregSourceProfilesActivityInput
	env.RegisterActivityWithOptions(func(input NormalizeBrregSourceProfilesActivityInput) (NormalizeBrregSourceProfilesActivityResult, error) {
		activityInput = input
		return NormalizeBrregSourceProfilesActivityResult{
			RecordsSeen:        2,
			CompaniesUpserted:  2,
			AddressesUpserted:  2,
			IndustriesUpserted: 2,
			ContactsUpserted:   1,
			CapitalUpserted:    1,
		}, nil
	}, activity.RegisterOptions{Name: normalizeBrregSourceProfilesActivity})

	env.ExecuteWorkflow(NormalizeBrregSourceProfiles, NormalizeBrregSourceProfilesInput{
		IDs:     []string{"11111111-1111-1111-1111-111111111111"},
		Filters: map[string]string{"query": "BORTIGARD"},
		Limit:   2,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"11111111-1111-1111-1111-111111111111"}, activityInput.IDs)
	require.Equal(t, map[string]string{"query": "BORTIGARD"}, activityInput.Filters)
	require.EqualValues(t, 2, activityInput.Limit)
	require.Equal(t, "manual", activityInput.Trigger)

	var result NormalizeBrregSourceProfilesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 2, result.RecordsSeen)
	require.EqualValues(t, 2, result.CompaniesUpserted)
	require.EqualValues(t, 2, result.AddressesUpserted)
	require.EqualValues(t, 2, result.IndustriesUpserted)
	require.EqualValues(t, 1, result.ContactsUpserted)
	require.EqualValues(t, 1, result.CapitalUpserted)
}

func TestNormalizeBrregSourceProfilesAllowsAllRecords(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(NormalizeBrregSourceProfiles)
	var activityInput NormalizeBrregSourceProfilesActivityInput
	env.RegisterActivityWithOptions(func(input NormalizeBrregSourceProfilesActivityInput) (NormalizeBrregSourceProfilesActivityResult, error) {
		activityInput = input
		return NormalizeBrregSourceProfilesActivityResult{
			RecordsSeen:       1200,
			CompaniesUpserted: 1200,
		}, nil
	}, activity.RegisterOptions{Name: normalizeBrregSourceProfilesActivity})

	env.ExecuteWorkflow(NormalizeBrregSourceProfiles, NormalizeBrregSourceProfilesInput{
		Limit:   0,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.EqualValues(t, 0, activityInput.Limit)

	var result NormalizeBrregSourceProfilesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 1200, result.RecordsSeen)
	require.EqualValues(t, 1200, result.CompaniesUpserted)
}
