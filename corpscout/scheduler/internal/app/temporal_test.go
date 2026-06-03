package app

import (
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
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

	if len(workers) != 8 {
		t.Fatalf("expected 8 temporal workers, got %d", len(workers))
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

func TestSubscribeTermTranslationResultsRequiresNATSConnection(t *testing.T) {
	_, err := subscribeTermTranslationResults(nil, nil, "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "nats connection not available")
}

func TestSubscribeTermTranslationResultsUsesDefaultSubject(t *testing.T) {
	serverURL := startAppNATSServer(t)
	conn, err := nats.Connect(serverURL)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	subscription, err := subscribeTermTranslationResults(nil, conn, "")

	require.NoError(t, err)
	require.True(t, subscription.IsValid())
	require.Equal(t, 1, conn.NumSubscriptions())
	require.Equal(t, translationclient.DefaultTermTranslationResultSubject, subscription.Subject)
	require.NoError(t, subscription.Unsubscribe())
	require.NoError(t, conn.FlushTimeout(2*time.Second))
	require.False(t, subscription.IsValid())
}

func startAppNATSServer(t *testing.T) string {
	t.Helper()
	server, err := natsserver.NewServer(&natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	})
	require.NoError(t, err)
	go server.Start()
	require.True(t, server.ReadyForConnections(5*time.Second))
	t.Cleanup(server.Shutdown)
	return server.ClientURL()
}
