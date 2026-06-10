package app

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
	"github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

func TestNewTemporalWorkersCreatesReferenceDataWorkers(t *testing.T) {
	temporalClient, err := client.NewLazyClient(client.Options{
		HostPort:  "localhost:7233",
		Namespace: "corpscout",
	})
	if err != nil {
		t.Fatalf("create lazy temporal client: %v", err)
	}
	defer temporalClient.Close()

	resources, err := newTemporalWorkerResources(config.Config{
		SourceRunsRoot:      t.TempDir(),
		ClickHouseNativeURL: "clickhouse://localhost:9000?database=test",
	}, nil, nil, nil)
	require.NoError(t, err)

	workers := newTemporalWorkers(temporalClient, resources)
	defer stopTemporalWorkers(workers)

	if len(workers) != 3 {
		t.Fatalf("expected 3 temporal workers, got %d", len(workers))
	}
}

func TestNewCompanySourcesTemporalWorkerRegistersDirectly(t *testing.T) {
	temporalClient, err := client.NewLazyClient(client.Options{
		HostPort:  "localhost:7233",
		Namespace: "corpscout",
	})
	require.NoError(t, err)
	defer temporalClient.Close()

	resources, err := newTemporalWorkerResources(config.Config{
		SourceRunsRoot:      t.TempDir(),
		ClickHouseNativeURL: "clickhouse://localhost:9000?database=test",
	}, nil, nil, nil)
	require.NoError(t, err)

	worker := newCompanySourcesTemporalWorker(temporalClient, resources)
	defer worker.Stop()
	require.NotNil(t, worker)
	require.Equal(t, companysources.SourceTaskQueue, "corpscout-company-sources")
}
