package translationqueue

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
)

type jetStreamPublisher interface {
	Publish(context.Context, string, []byte) error
}

type JetStreamClient struct {
	publisher jetStreamPublisher
	conn      *nats.Conn
}

func NewJetStreamClient(ctx context.Context, url string) (*JetStreamClient, error) {
	conn, err := nats.Connect(url, nats.Timeout(10*time.Second))
	if err != nil {
		return nil, errors.Wrap(err, "connect translation jetstream nats")
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "create translation jetstream context")
	}
	if err := ensureTranslationStream(ctx, js); err != nil {
		conn.Close()
		return nil, err
	}
	return &JetStreamClient{publisher: natsJetStreamPublisher{js: js}, conn: conn}, nil
}

func NewJetStreamClientFromPublisher(publisher jetStreamPublisher) *JetStreamClient {
	return &JetStreamClient{publisher: publisher}
}

func (c *JetStreamClient) PublishJob(ctx context.Context, job TranslationJob) error {
	body, err := encodeTranslationJob(job)
	if err != nil {
		return err
	}
	if c == nil || c.publisher == nil {
		return errors.New("translation jetstream publisher is required")
	}
	if err := c.publisher.Publish(ctx, JobsSubject, body); err != nil {
		return errors.Wrap(err, "publish translation jetstream job")
	}
	return nil
}

func encodeTranslationJob(job TranslationJob) ([]byte, error) {
	if strings.TrimSpace(job.BatchID) == "" {
		return nil, errors.New("translation job batch id is required")
	}
	if strings.TrimSpace(job.Source) == "" {
		return nil, errors.New("translation job source is required")
	}
	if len(job.Terms) == 0 {
		return nil, errors.New("translation job terms are required")
	}
	body, err := json.Marshal(job)
	if err != nil {
		return nil, errors.Wrap(err, "encode translation jetstream job")
	}
	return body, nil
}

func (c *JetStreamClient) Close() {
	if c != nil && c.conn != nil {
		c.conn.Close()
	}
}

func ensureTranslationStream(ctx context.Context, js nats.JetStreamContext) error {
	cfg := &nats.StreamConfig{
		Name:     StreamName,
		Subjects: []string{JobsSubject, ResultsSubject},
		Storage:  nats.FileStorage,
	}
	if _, err := js.AddStream(cfg, nats.Context(ctx)); err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return errors.Wrap(err, "ensure translation jetstream stream")
	}
	return nil
}

type natsJetStreamPublisher struct {
	js nats.JetStreamContext
}

func (p natsJetStreamPublisher) Publish(ctx context.Context, subject string, payload []byte) error {
	_, err := p.js.Publish(subject, payload, nats.Context(ctx))
	return err
}
