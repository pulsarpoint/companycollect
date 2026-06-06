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

type ResultMessage interface {
	Result() TranslationResult
	Ack(context.Context) error
}

type ResultConsumer interface {
	FetchResults(context.Context, int) ([]ResultMessage, error)
}

type JetStreamClient struct {
	publisher      jetStreamPublisher
	resultConsumer ResultConsumer
	conn           *nats.Conn
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
	resultSub, err := js.PullSubscribe(
		ResultsSubject,
		"scheduler-source-translation-results",
		nats.BindStream(StreamName),
		nats.ManualAck(),
	)
	if err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "create translation result pull subscription")
	}
	return &JetStreamClient{
		publisher:      natsJetStreamPublisher{js: js},
		resultConsumer: natsResultConsumer{sub: resultSub},
		conn:           conn,
	}, nil
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

func (c *JetStreamClient) FetchResults(ctx context.Context, batch int) ([]ResultMessage, error) {
	if c == nil || c.resultConsumer == nil {
		return nil, errors.New("translation result consumer is required")
	}
	return c.resultConsumer.FetchResults(ctx, batch)
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

type natsResultConsumer struct {
	sub *nats.Subscription
}

func (c natsResultConsumer) FetchResults(ctx context.Context, batch int) ([]ResultMessage, error) {
	if c.sub == nil {
		return nil, errors.New("translation result pull subscription is required")
	}
	if batch <= 0 {
		batch = 1
	}
	messages, err := c.sub.Fetch(batch, nats.Context(ctx), nats.MaxWait(time.Second))
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "fetch translation result messages")
	}
	results := make([]ResultMessage, 0, len(messages))
	for _, message := range messages {
		var decoded TranslationResult
		if err := json.Unmarshal(message.Data, &decoded); err != nil {
			_ = message.Ack()
			continue
		}
		results = append(results, natsResultMessage{message: message, result: decoded})
	}
	return results, nil
}

type natsResultMessage struct {
	message *nats.Msg
	result  TranslationResult
}

func (m natsResultMessage) Result() TranslationResult {
	return m.result
}

func (m natsResultMessage) Ack(context.Context) error {
	return errors.Wrap(m.message.Ack(), "ack translation result message")
}
