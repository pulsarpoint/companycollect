package translationqueue

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Service struct {
	dispatcher *Dispatcher
	collector  *ResultCollector
	interval   time.Duration
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewService(dispatcher *Dispatcher, collector *ResultCollector, interval time.Duration) *Service {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &Service{dispatcher: dispatcher, collector: collector, interval: interval}
}

func (s *Service) Start(ctx context.Context) {
	if s == nil || s.dispatcher == nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
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

func (s *Service) Stop() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}
