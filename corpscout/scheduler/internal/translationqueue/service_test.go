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

func TestResultServiceHandlesResultAndAcksAfterCollectorSuccess(t *testing.T) {
	message := &fakeResultMessage{
		result: TranslationResult{
			BatchID:       "batch-1",
			Source:        "brreg",
			Status:        "failed",
			PromptVersion: "v1",
		},
	}
	source := &collectorSourceStub{name: "brreg"}
	service := NewResultService(NewResultCollector(NewSourceRegistry(source)), &fakeResultConsumer{
		messages: []ResultMessage{message},
	})

	err := service.DrainOnce(context.Background())

	require.NoError(t, err)
	require.True(t, message.acked)
	require.Equal(t, int32(1), source.released)
}

type fakeResultConsumer struct {
	messages []ResultMessage
}

func (c *fakeResultConsumer) FetchResults(context.Context, int) ([]ResultMessage, error) {
	messages := c.messages
	c.messages = nil
	return messages, nil
}

type fakeResultMessage struct {
	result TranslationResult
	acked  bool
}

func (m *fakeResultMessage) Result() TranslationResult { return m.result }

func (m *fakeResultMessage) Ack(context.Context) error {
	m.acked = true
	return nil
}
