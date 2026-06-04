package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestNormalizeAriregisterSourceProfilesWithCopyRunsSingleActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(NormalizeAriregisterSourceProfilesWithCopy)
	var activityInput NormalizeAriregisterSourceProfilesActivityInput
	env.RegisterActivityWithOptions(func(input NormalizeAriregisterSourceProfilesActivityInput) (NormalizeAriregisterSourceProfilesActivityResult, error) {
		activityInput = input
		return NormalizeAriregisterSourceProfilesActivityResult{
			RecordsSeen:                  2,
			CompaniesUpserted:            2,
			CompanyNamesUpserted:         2,
			CompanyStatusesUpserted:      2,
			LegalFormsUpserted:           2,
			AddressesUpserted:            2,
			ContactsUpserted:             1,
			WebsitesUpserted:             1,
			DomainsUpserted:              1,
			IndustriesUpserted:           2,
			CapitalUpserted:              1,
			FinancialYearPeriodsUpserted: 1,
			AnnualReportsUpserted:        1,
			ArticlesUpserted:             1,
			RegistryNotesUpserted:        1,
		}, nil
	}, activity.RegisterOptions{Name: NormalizeAriregisterSourceProfilesWithCopyActivity})

	env.ExecuteWorkflow(NormalizeAriregisterSourceProfilesWithCopy, NormalizeAriregisterSourceProfilesInput{
		IDs:     []string{"11111111-1111-1111-1111-111111111111"},
		Filters: map[string]string{"query": "AKTSIASELTS"},
		Limit:   2,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"11111111-1111-1111-1111-111111111111"}, activityInput.IDs)
	require.Equal(t, map[string]string{"query": "AKTSIASELTS"}, activityInput.Filters)
	require.EqualValues(t, 2, activityInput.Limit)
	require.Equal(t, "manual", activityInput.Trigger)

	var result NormalizeAriregisterSourceProfilesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 2, result.RecordsSeen)
	require.EqualValues(t, 2, result.CompaniesUpserted)
	require.EqualValues(t, 2, result.CompanyNamesUpserted)
	require.EqualValues(t, 2, result.CompanyStatusesUpserted)
	require.EqualValues(t, 2, result.LegalFormsUpserted)
	require.EqualValues(t, 2, result.AddressesUpserted)
	require.EqualValues(t, 1, result.ContactsUpserted)
	require.EqualValues(t, 1, result.WebsitesUpserted)
	require.EqualValues(t, 1, result.DomainsUpserted)
	require.EqualValues(t, 2, result.IndustriesUpserted)
	require.EqualValues(t, 1, result.CapitalUpserted)
	require.EqualValues(t, 1, result.FinancialYearPeriodsUpserted)
	require.EqualValues(t, 1, result.AnnualReportsUpserted)
	require.EqualValues(t, 1, result.ArticlesUpserted)
	require.EqualValues(t, 1, result.RegistryNotesUpserted)
}

func TestNormalizeAriregisterSourceProfilesWithCopyProcessesAllRecordsInChunks(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(NormalizeAriregisterSourceProfilesWithCopy)
	var activityInputs []NormalizeAriregisterSourceProfilesActivityInput
	env.RegisterActivityWithOptions(func(input NormalizeAriregisterSourceProfilesActivityInput) (NormalizeAriregisterSourceProfilesActivityResult, error) {
		activityInputs = append(activityInputs, input)
		switch len(activityInputs) {
		case 1, 2:
			return NormalizeAriregisterSourceProfilesActivityResult{
				RecordsSeen:          5000,
				CompaniesUpserted:    5000,
				CompanyNamesUpserted: 5000,
			}, nil
		default:
			return NormalizeAriregisterSourceProfilesActivityResult{}, nil
		}
	}, activity.RegisterOptions{Name: NormalizeAriregisterSourceProfilesWithCopyActivity})

	env.ExecuteWorkflow(NormalizeAriregisterSourceProfilesWithCopy, NormalizeAriregisterSourceProfilesInput{
		Limit:   0,
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, activityInputs, 3)
	require.EqualValues(t, defaultSourceProfileChunkSize, activityInputs[0].Limit)
	require.EqualValues(t, defaultSourceProfileChunkSize, activityInputs[1].Limit)
	require.EqualValues(t, defaultSourceProfileChunkSize, activityInputs[2].Limit)

	var result NormalizeAriregisterSourceProfilesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 10000, result.RecordsSeen)
	require.EqualValues(t, 10000, result.CompaniesUpserted)
	require.EqualValues(t, 10000, result.CompanyNamesUpserted)
	require.EqualValues(t, 2, result.ChunksProcessed)
}

func TestNormalizeAriregisterSourceProfilesWithCopyRespectsLimitAcrossChunks(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(NormalizeAriregisterSourceProfilesWithCopy)
	var activityInputs []NormalizeAriregisterSourceProfilesActivityInput
	env.RegisterActivityWithOptions(func(input NormalizeAriregisterSourceProfilesActivityInput) (NormalizeAriregisterSourceProfilesActivityResult, error) {
		activityInputs = append(activityInputs, input)
		if len(activityInputs) == 1 {
			return NormalizeAriregisterSourceProfilesActivityResult{
				RecordsSeen:       5000,
				CompaniesUpserted: 5000,
			}, nil
		}
		return NormalizeAriregisterSourceProfilesActivityResult{
			RecordsSeen:       2500,
			CompaniesUpserted: 2500,
		}, nil
	}, activity.RegisterOptions{Name: NormalizeAriregisterSourceProfilesWithCopyActivity})

	env.ExecuteWorkflow(NormalizeAriregisterSourceProfilesWithCopy, NormalizeAriregisterSourceProfilesInput{
		Limit:     7500,
		BatchSize: 5000,
		Trigger:   "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, activityInputs, 2)
	require.EqualValues(t, 5000, activityInputs[0].Limit)
	require.EqualValues(t, 2500, activityInputs[1].Limit)

	var result NormalizeAriregisterSourceProfilesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 7500, result.RecordsSeen)
	require.EqualValues(t, 7500, result.CompaniesUpserted)
	require.EqualValues(t, 2, result.ChunksProcessed)
}

func TestRefreshAriregisterSourceExplorerRunsSingleActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(RefreshAriregisterSourceExplorer)
	var activityInput RefreshAriregisterSourceExplorerActivityInput
	env.RegisterActivityWithOptions(func(input RefreshAriregisterSourceExplorerActivityInput) (RefreshAriregisterSourceExplorerActivityResult, error) {
		activityInput = input
		latest := "2026-06-04 06:33:17+00"
		return RefreshAriregisterSourceExplorerActivityResult{
			Refreshed:             true,
			UsedConcurrentRefresh: true,
			SourceEntries:         1000,
			LatestSourceUpdatedAt: &latest,
		}, nil
	}, activity.RegisterOptions{Name: RefreshAriregisterSourceExplorerActivity})

	env.ExecuteWorkflow(RefreshAriregisterSourceExplorer, RefreshAriregisterSourceExplorerInput{
		Trigger: "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "manual", activityInput.Trigger)

	var result RefreshAriregisterSourceExplorerResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "succeeded", result.Status)
	require.True(t, result.Refreshed)
	require.True(t, result.UsedConcurrentRefresh)
	require.EqualValues(t, 1000, result.SourceEntries)
	require.NotNil(t, result.LatestSourceUpdatedAt)
}
