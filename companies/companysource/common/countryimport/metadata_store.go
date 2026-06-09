package countryimport

import "context"

type MetadataStore interface {
	SaveDownload(ctx context.Context, metadata DownloadMetadata) error
	SaveProcess(ctx context.Context, metadata ProcessMetadata) error
}

type NoopMetadataStore struct{}

func (NoopMetadataStore) SaveDownload(ctx context.Context, metadata DownloadMetadata) error {
	return nil
}

func (NoopMetadataStore) SaveProcess(ctx context.Context, metadata ProcessMetadata) error {
	return nil
}
