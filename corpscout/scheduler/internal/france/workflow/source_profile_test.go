package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestNormalizeFranceSourceProfilesRunsSingleActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(NormalizeFranceSourceProfiles)
	var activityInput NormalizeFranceSourceProfilesActivityInput
	env.RegisterActivityWithOptions(func(input NormalizeFranceSourceProfilesActivityInput) (NormalizeFranceSourceProfilesActivityResult, error) {
		activityInput = input
		return NormalizeFranceSourceProfilesActivityResult{
			RecordsSeen:            2,
			CompaniesUpserted:      2,
			EstablishmentsUpserted: 4,
			AddressesUpserted:      4,
			IndustriesInserted:     6,
			WebsitesUpserted:       0,
			DomainsUpserted:        0,
			ContactsUpserted:       0,
		}, nil
	}, activity.RegisterOptions{Name: NormalizeFranceSourceProfilesActivity})

	env.ExecuteWorkflow(NormalizeFranceSourceProfiles, NormalizeFranceSourceProfilesInput{
		IDs:       []string{"11111111-1111-1111-1111-111111111111"},
		Filters:   map[string]string{"query": "PULSAR"},
		Limit:     2,
		BatchSize: 10,
		Trigger:   "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"11111111-1111-1111-1111-111111111111"}, activityInput.IDs)
	require.Equal(t, map[string]string{"query": "PULSAR"}, activityInput.Filters)
	require.EqualValues(t, 2, activityInput.Limit)
	require.Equal(t, "manual", activityInput.Trigger)

	var result NormalizeFranceSourceProfilesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 1, result.ChunksProcessed)
	require.EqualValues(t, 2, result.RecordsSeen)
	require.EqualValues(t, 2, result.CompaniesUpserted)
	require.EqualValues(t, 4, result.EstablishmentsUpserted)
	require.EqualValues(t, 4, result.AddressesUpserted)
	require.EqualValues(t, 6, result.IndustriesInserted)
}

func TestNormalizeFranceSourceProfilesProcessesAllRecordsInChunks(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(NormalizeFranceSourceProfiles)
	var activityInputs []NormalizeFranceSourceProfilesActivityInput
	env.RegisterActivityWithOptions(func(input NormalizeFranceSourceProfilesActivityInput) (NormalizeFranceSourceProfilesActivityResult, error) {
		activityInputs = append(activityInputs, input)
		if len(activityInputs) < 3 {
			return NormalizeFranceSourceProfilesActivityResult{
				RecordsSeen:       defaultFranceSourceProfileChunkSize,
				CompaniesUpserted: defaultFranceSourceProfileChunkSize,
			}, nil
		}
		return NormalizeFranceSourceProfilesActivityResult{}, nil
	}, activity.RegisterOptions{Name: NormalizeFranceSourceProfilesActivity})

	env.ExecuteWorkflow(NormalizeFranceSourceProfiles, NormalizeFranceSourceProfilesInput{
		Limit:   0,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, activityInputs, 3)
	require.EqualValues(t, defaultFranceSourceProfileChunkSize, activityInputs[0].Limit)
	require.EqualValues(t, defaultFranceSourceProfileChunkSize, activityInputs[1].Limit)
	require.EqualValues(t, defaultFranceSourceProfileChunkSize, activityInputs[2].Limit)

	var result NormalizeFranceSourceProfilesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, defaultFranceSourceProfileChunkSize*2, result.RecordsSeen)
	require.EqualValues(t, 2, result.ChunksProcessed)
}

func TestNormalizeFranceSourceProfilesRejectsNegativeLimit(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(NormalizeFranceSourceProfiles)
	activityCalls := 0
	env.RegisterActivityWithOptions(func(input NormalizeFranceSourceProfilesActivityInput) (NormalizeFranceSourceProfilesActivityResult, error) {
		activityCalls++
		return NormalizeFranceSourceProfilesActivityResult{}, nil
	}, activity.RegisterOptions{Name: NormalizeFranceSourceProfilesActivity})

	env.ExecuteWorkflow(NormalizeFranceSourceProfiles, NormalizeFranceSourceProfilesInput{
		Limit: -1,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "limit cannot be negative")
	require.Zero(t, activityCalls)
}
