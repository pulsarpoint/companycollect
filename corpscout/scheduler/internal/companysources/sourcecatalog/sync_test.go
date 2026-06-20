package sourcecatalog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type fakeStore struct {
	upserts       []db.UpsertDataSourceFromCatalogParams
	fileUpserts   []db.UpsertDataSourceFileFromCatalogParams
	actionUpserts []db.UpsertDataSourceActionFromCatalogParams
	disabled      map[string][]string
	pruned        []string
}

func (s *fakeStore) UpsertDataSourceActionFromCatalog(ctx context.Context, arg db.UpsertDataSourceActionFromCatalogParams) error {
	s.actionUpserts = append(s.actionUpserts, arg)
	return nil
}

func (s *fakeStore) UpsertDataSourceFromCatalog(ctx context.Context, arg db.UpsertDataSourceFromCatalogParams) error {
	s.upserts = append(s.upserts, arg)
	return nil
}

func (s *fakeStore) UpsertDataSourceFileFromCatalog(ctx context.Context, arg db.UpsertDataSourceFileFromCatalogParams) error {
	s.fileUpserts = append(s.fileUpserts, arg)
	return nil
}

func (s *fakeStore) DisableDataSourceFilesNotInCatalog(ctx context.Context, arg db.DisableDataSourceFilesNotInCatalogParams) error {
	if s.disabled == nil {
		s.disabled = map[string][]string{}
	}
	s.disabled[arg.RegistryKey] = append([]string(nil), arg.FileKeys...)
	return nil
}

func (s *fakeStore) PruneDataSourcesNotInCatalog(ctx context.Context, registryKeys []string) error {
	s.pruned = append([]string(nil), registryKeys...)
	return nil
}

func TestSyncUpsertsAndPrunesCatalogSources(t *testing.T) {
	specs, err := LoadEmbedded()
	require.NoError(t, err)

	store := &fakeStore{}
	require.NoError(t, Sync(context.Background(), store, specs))

	require.Len(t, store.upserts, 5)
	require.NotEmpty(t, store.fileUpserts)
	require.NotEmpty(t, store.actionUpserts)
	require.Contains(t, store.disabled, "finland/prhytj")
	require.Contains(t, store.disabled["finland/prhytj"], "source")
	require.Contains(t, store.disabled, "finland/prh_xbrl")
	require.Contains(t, store.disabled["finland/prh_xbrl"], "statements_manifest")
	require.ElementsMatch(t, []string{
		"finland/prhytj",
		"finland/prh_xbrl",
		"united_states/coloradoentities",
		"united_states/irseobmf",
		"united_states/secedgar",
	}, store.pruned)
	require.Equal(t, "corpscout", store.upserts[0].ClickhouseDatabase)

	var prhXBRLAction db.UpsertDataSourceActionFromCatalogParams
	var foundPRHXBRLAction bool
	for _, action := range store.actionUpserts {
		if action.RegistryKey == "finland/prh_xbrl" {
			prhXBRLAction = action
			foundPRHXBRLAction = true
		}
	}
	require.True(t, foundPRHXBRLAction)
	require.Equal(t, "pull_source", prhXBRLAction.Action)
	require.Equal(t, "CompanySourceDownloadWorkflow", prhXBRLAction.TemporalWorkflowType)
	require.True(t, prhXBRLAction.Enabled)
}
