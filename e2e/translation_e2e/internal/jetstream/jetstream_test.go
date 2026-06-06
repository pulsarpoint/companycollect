package jetstream

import (
	"testing"

	"github.com/nats-io/nats.go"
)

func TestInputConsumerDepthCountsPendingAndAckPendingMessages(t *testing.T) {
	info := &nats.ConsumerInfo{
		NumPending:    7,
		NumAckPending: 3,
	}

	if got := inputConsumerDepth(info); got != 10 {
		t.Fatalf("input consumer depth = %d, want 10", got)
	}
}
