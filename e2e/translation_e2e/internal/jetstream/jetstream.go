package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/config"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/contracts"
)

type Harness struct {
	conn      *nats.Conn
	js        nats.JetStreamContext
	resultSub *nats.Subscription
	cfg       config.Config
}

type ResultMessage struct {
	Result contracts.TranslationResult
	Ack    func(context.Context) error
}

func New(ctx context.Context, cfg config.Config) (*Harness, error) {
	conn, err := nats.Connect(cfg.NATSURL, nats.Timeout(10*time.Second))
	if err != nil {
		return nil, errors.Wrap(err, "connect nats")
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "create jetstream context")
	}
	harness := &Harness{conn: conn, js: js, cfg: cfg}
	if err := harness.ensureStreams(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	if cfg.Purge {
		if err := harness.purgeStreams(ctx); err != nil {
			conn.Close()
			return nil, err
		}
	}
	_ = js.DeleteConsumer(cfg.OutputStream, cfg.ResultDurable)
	resultSub, err := js.PullSubscribe(
		cfg.OutputQueue,
		cfg.ResultDurable,
		nats.BindStream(cfg.OutputStream),
		nats.DeliverNew(),
		nats.ManualAck(),
	)
	if err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "create output result pull subscription")
	}
	harness.resultSub = resultSub
	return harness, nil
}

func (h *Harness) InputDepth(ctx context.Context) (uint64, error) {
	if h.cfg.InputConsumer != "" {
		info, err := h.js.ConsumerInfo(h.cfg.InputStream, h.cfg.InputConsumer, nats.Context(ctx))
		if err == nil {
			return inputConsumerDepth(info), nil
		}
		if !errors.Is(err, nats.ErrConsumerNotFound) {
			return 0, errors.Wrap(err, "read input consumer info")
		}
	}
	info, err := h.js.StreamInfo(
		h.cfg.InputStream,
		&nats.StreamInfoRequest{SubjectsFilter: h.cfg.InputQueue},
		nats.Context(ctx),
	)
	if err != nil {
		return 0, errors.Wrap(err, "read input stream info")
	}
	return info.State.Subjects[h.cfg.InputQueue], nil
}

func inputConsumerDepth(info *nats.ConsumerInfo) uint64 {
	if info == nil {
		return 0
	}
	depth := info.NumPending
	if info.NumAckPending > 0 {
		depth += uint64(info.NumAckPending)
	}
	return depth
}

func (h *Harness) PublishJob(ctx context.Context, job contracts.TranslationJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return errors.Wrap(err, "encode translation job")
	}
	if _, err := h.js.Publish(h.cfg.InputQueue, body, nats.Context(ctx)); err != nil {
		return errors.Wrap(err, "publish translation job")
	}
	return nil
}

func (h *Harness) FetchResults(ctx context.Context, max int, maxWait time.Duration) ([]ResultMessage, error) {
	if max <= 0 {
		max = 1
	}
	fetchCtx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	messages, err := h.resultSub.Fetch(max, nats.Context(fetchCtx))
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
			return nil, nil
		}
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return nil, nil
		}
		return nil, errors.Wrap(err, "fetch translation results")
	}
	results := make([]ResultMessage, 0, len(messages))
	for _, message := range messages {
		var decoded contracts.TranslationResult
		if err := json.Unmarshal(message.Data, &decoded); err != nil {
			return nil, errors.Wrap(err, "decode translation result")
		}
		msg := message
		results = append(results, ResultMessage{
			Result: decoded,
			Ack: func(context.Context) error {
				return errors.Wrap(msg.Ack(), "ack translation result")
			},
		})
	}
	return results, nil
}

func (h *Harness) Close() {
	if h != nil && h.conn != nil {
		h.conn.Close()
	}
}

func (h *Harness) ensureStreams(ctx context.Context) error {
	if h.cfg.InputStream == h.cfg.OutputStream {
		return h.ensureStream(ctx, h.cfg.InputStream, []string{h.cfg.InputQueue, h.cfg.OutputQueue}, false)
	}
	if err := h.ensureWorkQueueStream(ctx, h.cfg.InputStream, h.cfg.InputQueue); err != nil {
		return err
	}
	if err := h.ensureWorkQueueStream(ctx, h.cfg.OutputStream, h.cfg.OutputQueue); err != nil {
		return err
	}
	return nil
}

func (h *Harness) ensureWorkQueueStream(ctx context.Context, stream string, subject string) error {
	return h.ensureStream(ctx, stream, []string{subject}, true)
}

func (h *Harness) ensureStream(ctx context.Context, stream string, subjects []string, requireWorkQueue bool) error {
	info, err := h.js.StreamInfo(stream, nats.Context(ctx))
	if err == nil {
		if requireWorkQueue && info.Config.Retention != nats.WorkQueuePolicy {
			return fmt.Errorf("stream %s retention is %s, want workqueue; delete or recreate the stream before running E2E", stream, info.Config.Retention)
		}
		for _, subject := range subjects {
			if !slices.Contains(info.Config.Subjects, subject) {
				return fmt.Errorf("stream %s subjects %v do not include %s", stream, info.Config.Subjects, subject)
			}
		}
		return nil
	}
	if !errors.Is(err, nats.ErrStreamNotFound) {
		return errors.Wrapf(err, "read stream %s", stream)
	}
	streamConfig := &nats.StreamConfig{
		Name:     stream,
		Subjects: subjects,
		Storage:  nats.FileStorage,
		MaxAge:   30 * time.Minute,
	}
	if requireWorkQueue {
		streamConfig.Retention = nats.WorkQueuePolicy
	}
	_, err = h.js.AddStream(streamConfig, nats.Context(ctx))
	if err != nil {
		return errors.Wrapf(err, "create stream %s", stream)
	}
	return nil
}

func (h *Harness) purgeStreams(ctx context.Context) error {
	if err := h.js.PurgeStream(h.cfg.InputStream, nats.Context(ctx)); err != nil {
		return errors.Wrap(err, "purge input stream")
	}
	if h.cfg.InputStream == h.cfg.OutputStream {
		return nil
	}
	if err := h.js.PurgeStream(h.cfg.OutputStream, nats.Context(ctx)); err != nil {
		return errors.Wrap(err, "purge output stream")
	}
	return nil
}
