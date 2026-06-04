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

func TestNormalizeBrregSourceProfilesProcessesAllRecordsInChunks(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(NormalizeBrregSourceProfiles)
	var activityInputs []NormalizeBrregSourceProfilesActivityInput
	env.RegisterActivityWithOptions(func(input NormalizeBrregSourceProfilesActivityInput) (NormalizeBrregSourceProfilesActivityResult, error) {
		activityInputs = append(activityInputs, input)
		switch len(activityInputs) {
		case 1, 2:
			return NormalizeBrregSourceProfilesActivityResult{
				RecordsSeen:       5000,
				CompaniesUpserted: 5000,
			}, nil
		default:
			return NormalizeBrregSourceProfilesActivityResult{}, nil
		}
	}, activity.RegisterOptions{Name: normalizeBrregSourceProfilesActivity})

	env.ExecuteWorkflow(NormalizeBrregSourceProfiles, NormalizeBrregSourceProfilesInput{
		Limit:   0,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, activityInputs, 3)
	require.EqualValues(t, defaultSourceProfileChunkSize, activityInputs[0].Limit)
	require.EqualValues(t, defaultSourceProfileChunkSize, activityInputs[1].Limit)
	require.EqualValues(t, defaultSourceProfileChunkSize, activityInputs[2].Limit)

	var result NormalizeBrregSourceProfilesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 10000, result.RecordsSeen)
	require.EqualValues(t, 10000, result.CompaniesUpserted)
	require.EqualValues(t, 2, result.ChunksProcessed)
}

func TestNormalizeBrregSourceProfilesWithCopyRunsCopyActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(NormalizeBrregSourceProfilesWithCopy)
	var activityInput NormalizeBrregSourceProfilesActivityInput
	env.RegisterActivityWithOptions(func(input NormalizeBrregSourceProfilesActivityInput) (NormalizeBrregSourceProfilesActivityResult, error) {
		activityInput = input
		return NormalizeBrregSourceProfilesActivityResult{
			RecordsSeen:       3,
			CompaniesUpserted: 3,
		}, nil
	}, activity.RegisterOptions{Name: normalizeBrregSourceProfilesWithCopyActivity})

	env.ExecuteWorkflow(NormalizeBrregSourceProfilesWithCopy, NormalizeBrregSourceProfilesInput{
		Limit:     3,
		BatchSize: 1000,
		Trigger:   "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.EqualValues(t, 3, activityInput.Limit)
	require.Equal(t, "manual", activityInput.Trigger)

	var result NormalizeBrregSourceProfilesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 3, result.RecordsSeen)
	require.EqualValues(t, 3, result.CompaniesUpserted)
}

func TestRefreshBrregSourceExplorerRunsSingleActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(RefreshBrregSourceExplorer)
	var activityInput RefreshBrregSourceExplorerActivityInput
	env.RegisterActivityWithOptions(func(input RefreshBrregSourceExplorerActivityInput) (RefreshBrregSourceExplorerActivityResult, error) {
		activityInput = input
		latest := "2026-06-04 06:33:17+00"
		return RefreshBrregSourceExplorerActivityResult{
			Refreshed:             true,
			UsedConcurrentRefresh: true,
			SourceEntries:         1000,
			LatestSourceUpdatedAt: &latest,
		}, nil
	}, activity.RegisterOptions{Name: refreshBrregSourceExplorerActivity})

	env.ExecuteWorkflow(RefreshBrregSourceExplorer, RefreshBrregSourceExplorerInput{
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "manual", activityInput.Trigger)

	var result RefreshBrregSourceExplorerResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.True(t, result.Refreshed)
	require.True(t, result.UsedConcurrentRefresh)
	require.EqualValues(t, 1000, result.SourceEntries)
	require.NotNil(t, result.LatestSourceUpdatedAt)
}
