package app

import (
	"strings"
	"testing"

	"go.temporal.io/sdk/client"

	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
)

func TestNewTemporalWorkersCreatesBrregWorkers(t *testing.T) {
	temporalClient, err := client.NewLazyClient(client.Options{
		HostPort:  "localhost:7233",
		Namespace: "corpscout",
	})
	if err != nil {
		t.Fatalf("create lazy temporal client: %v", err)
	}
	defer temporalClient.Close()

	workers := newTemporalWorkers(temporalClient, &temporalWorkerResources{})
	defer stopTemporalWorkers(workers)

	if len(workers) != 16 {
		t.Fatalf("expected 16 temporal workers, got %d", len(workers))
	}
}

func TestNewTemporalWorkerResourcesRequiresNATSURL(t *testing.T) {
	_, err := newTemporalWorkerResources(config.Config{}, nil, nil, nil)

	if err == nil {
		t.Fatal("expected missing nats url error")
	}
	if !strings.Contains(err.Error(), "CORPSCOUT_NATS_URL is required") {
		t.Fatalf("expected nats url error, got %v", err)
	}
}
