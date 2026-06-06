package translationqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServiceDefaultsDispatchInterval(t *testing.T) {
	service := NewService(
		NewDispatcher(SourceRegistry{}, &dispatcherJobPublisherStub{}, DispatcherConfig{}),
		NewResultCollector(SourceRegistry{}),
		0,
	)

	require.Equal(t, 2*time.Second, service.interval)
}

func TestServiceStartStopWithEmptyRegistry(t *testing.T) {
	dispatcher := NewDispatcher(NewSourceRegistry(), &dispatcherJobPublisherStub{}, DispatcherConfig{})
	service := NewService(dispatcher, NewResultCollector(NewSourceRegistry()), time.Millisecond)

	service.Start(context.Background())
	service.Stop()
}
