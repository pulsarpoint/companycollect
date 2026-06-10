package sourcecatalog

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/errors"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type Store interface {
	UpsertDataSourceFromCatalog(ctx context.Context, arg db.UpsertDataSourceFromCatalogParams) error
	UpsertDataSourceFileFromCatalog(ctx context.Context, arg db.UpsertDataSourceFileFromCatalogParams) error
	DisableDataSourceFilesNotInCatalog(ctx context.Context, arg db.DisableDataSourceFilesNotInCatalogParams) error
	PruneDataSourcesNotInCatalog(ctx context.Context, registryKeys []string) error
}

func Sync(ctx context.Context, store Store, specs []Spec) error {
	if len(specs) == 0 {
		return errors.New("source catalog must contain at least one source")
	}

	registryKeys := make([]string, 0, len(specs))
	for _, spec := range specs {
		if err := spec.Validate(); err != nil {
			return err
		}
		registryKeys = append(registryKeys, spec.RegistryKey)

		displayName := spec.DisplayName
		description := spec.Description
		sourceURL := spec.SourceURL
		docsURL := spec.DocsURL
		rawSourceRetention := spec.RawSourceRetention
		sourceFileName := spec.SourceFileName

		if err := store.UpsertDataSourceFromCatalog(ctx, db.UpsertDataSourceFromCatalogParams{
			Name:                  spec.Name,
			Country:               spec.Country,
			Source:                spec.Source,
			RegistryKey:           spec.RegistryKey,
			DisplayName:           &displayName,
			Description:           &description,
			SourceGroup:           spec.SourceGroup,
			InputTableName:        spec.InputTableName,
			Enabled:               spec.Enabled,
			AuthRequired:          spec.AuthRequired,
			StorageKind:           spec.StorageKind,
			ClickhouseDatabase:    spec.ClickHouseDatabase,
			ClickhouseTablePrefix: spec.ClickHouseTablePrefix,
			SourceUrl:             &sourceURL,
			DocsUrl:               &docsURL,
			RawSourceRetention:    &rawSourceRetention,
			SourceFileName:        &sourceFileName,
			UserAgentRequired:     spec.UserAgentRequired,
			Capabilities:          spec.Capabilities,
			RequiresTranslation:   spec.RequiresTranslation,
		}); err != nil {
			return errors.Wrapf(err, "upsert source catalog spec %s", spec.RegistryKey)
		}

		fileKeys := make([]string, 0, len(spec.Files))
		for _, file := range spec.Files {
			config := file.Config
			if config == nil {
				config = map[string]any{}
			}
			configData, err := json.Marshal(config)
			if err != nil {
				return errors.Wrapf(err, "marshal source file config %s/%s", spec.RegistryKey, file.FileKey)
			}

			if err := store.UpsertDataSourceFileFromCatalog(ctx, db.UpsertDataSourceFileFromCatalogParams{
				FileKey:      file.FileKey,
				DisplayName:  file.DisplayName,
				Description:  file.Description,
				Kind:         file.Kind,
				Required:     file.Required,
				RelativePath: file.RelativePath,
				Enabled:      file.Enabled,
				SortOrder:    file.SortOrder,
				Config:       configData,
				RegistryKey:  spec.RegistryKey,
			}); err != nil {
				return errors.Wrapf(err, "upsert source file catalog spec %s/%s", spec.RegistryKey, file.FileKey)
			}
			fileKeys = append(fileKeys, file.FileKey)
		}

		if err := store.DisableDataSourceFilesNotInCatalog(ctx, db.DisableDataSourceFilesNotInCatalogParams{
			RegistryKey: spec.RegistryKey,
			FileKeys:    fileKeys,
		}); err != nil {
			return errors.Wrapf(err, "disable source files not in catalog %s", spec.RegistryKey)
		}
	}

	return errors.Wrap(store.PruneDataSourcesNotInCatalog(ctx, registryKeys), "prune data sources not in catalog")
}
