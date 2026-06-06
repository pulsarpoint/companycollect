package translationqueue

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
)

type Service struct {
	dispatcher    *Dispatcher
	resultService *ResultService
	interval      time.Duration
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

func NewService(dispatcher *Dispatcher, collector *ResultCollector, interval time.Duration) *Service {
	return NewServiceWithResultConsumer(dispatcher, collector, nil, interval)
}

func NewServiceWithResultConsumer(
	dispatcher *Dispatcher,
	collector *ResultCollector,
	consumer ResultConsumer,
	interval time.Duration,
) *Service {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	var resultService *ResultService
	if collector != nil && consumer != nil {
		resultService = NewResultService(collector, consumer)
	}
	return &Service{dispatcher: dispatcher, resultService: resultService, interval: interval}
}

type ResultService struct {
	collector *ResultCollector
	consumer  ResultConsumer
}

func NewResultService(collector *ResultCollector, consumer ResultConsumer) *ResultService {
	return &ResultService{collector: collector, consumer: consumer}
}

func (s *ResultService) DrainOnce(ctx context.Context) error {
	if s == nil || s.collector == nil || s.consumer == nil {
		return errors.New("translation result service is not configured")
	}
	messages, err := s.consumer.FetchResults(ctx, 1)
	if err != nil {
		return errors.Wrap(err, "fetch translation result messages")
	}
	for _, message := range messages {
		if err := s.collector.HandleResult(ctx, message.Result()); err != nil {
			return errors.Wrap(err, "collect translation result")
		}
		if err := message.Ack(ctx); err != nil {
			return errors.Wrap(err, "ack translation result message")
		}
	}
	return nil
}

func (s *Service) Start(ctx context.Context) {
	if s == nil || (s.dispatcher == nil && s.resultService == nil) {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	if s.dispatcher != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			ticker := time.NewTicker(s.interval)
			defer ticker.Stop()
			for {
				if err := s.dispatcher.RefillOnce(runCtx); err != nil {
					slog.ErrorContext(runCtx, "refill translation jetstream buffer", "error", err)
				}
				select {
				case <-runCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	if s.resultService != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				if err := s.resultService.DrainOnce(runCtx); err != nil {
					if runCtx.Err() != nil {
						return
					}
					slog.ErrorContext(runCtx, "drain translation result messages", "error", err)
					if !waitForInterval(runCtx, s.interval) {
						return
					}
				}
				select {
				case <-runCtx.Done():
					return
				default:
				}
			}
		}()
	}
}

func (s *Service) Stop() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func waitForInterval(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
