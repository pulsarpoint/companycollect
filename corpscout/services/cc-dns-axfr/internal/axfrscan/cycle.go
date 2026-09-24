package axfrscan

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cc-dns-axfr/internal/axfrprobe"
	"cc-dns-axfr/internal/clickhouseconn"
	"cc-dns-axfr/internal/scheduler"

	"golang.org/x/sync/errgroup"
)

const axfrIdlePollInterval = 200 * time.Millisecond

// RunCycle completes or resumes one AXFR traversal using dbPath as its private durable state.
func RunCycle(ctx context.Context, dbPath string, config Config) error {
	config = config.withDefaults()
	store, err := openStore(dbPath)
	if err != nil {
		return err
	}
	defer store.close()
	if err := store.begin(ctx, config.ScanID, time.Now().UTC()); err != nil {
		return fmt.Errorf("begin AXFR cycle: %w", err)
	}
	if err := store.resetRunning(ctx, config.ScanID); err != nil {
		return fmt.Errorf("recover interrupted AXFR work: %w", err)
	}
	state, err := store.state(ctx, config.ScanID)
	if err != nil {
		return fmt.Errorf("read AXFR cycle state: %w", err)
	}

	probeScheduler := scheduler.New(scheduler.Config{
		PerServerQPS: config.PerServerQPS,
		Burst:        max(1, int(config.PerServerQPS)),
		MaxInFlight:  1,
	})
	prober := axfrprobe.NewAXFRProber(probeScheduler, axfrprobe.AXFRCaps{
		MaxRecords: config.MaxRecords,
		MaxBytes:   config.MaxBytes,
		Deadline:   config.Timeout,
	})

	group, groupContext := errgroup.WithContext(ctx)
	group.Go(func() error { return axfrSourceLoop(groupContext, store, config) })
	group.Go(func() error { return axfrWorkLoop(groupContext, store, config, prober) })
	group.Go(func() error { return axfrFlushLoop(groupContext, store, config) })
	if config.StatsInterval > 0 {
		group.Go(func() error { return axfrStatsLoop(groupContext, store, config, state.StartedAt) })
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return store.checkpoint(ctx)
}

func axfrSourceLoop(ctx context.Context, store *axfrStore, config Config) error {
	connection, err := clickhouseconn.Open()
	if err != nil {
		return fmt.Errorf("open AXFR domain source: %w", err)
	}
	defer connection.Close()
	for {
		state, err := store.state(ctx, config.ScanID)
		if err != nil {
			return fmt.Errorf("read AXFR source state: %w", err)
		}
		if state.SourceExhausted {
			return nil
		}
		active, err := store.activeDomains(ctx, config.ScanID)
		if err != nil {
			return fmt.Errorf("count active AXFR domains: %w", err)
		}
		available := config.WorkCapacity - active
		if available <= 0 {
			if err := axfrWait(ctx, axfrIdlePollInterval); err != nil {
				return err
			}
			continue
		}
		pageSize := min(config.DomainPageSize, available)
		if config.MaxDomains > 0 {
			remaining := config.MaxDomains - state.DomainsFetched
			if remaining <= 0 {
				_, err := store.addPage(ctx, config.ScanID, nil, true, config.MaxDomains)
				return err
			}
			pageSize = min(pageSize, remaining)
		}
		page, err := fetchSourcePage(ctx, connection, state.Cursor, pageSize)
		if err != nil {
			return err
		}
		exhausted := len(page) < pageSize
		if _, err := store.addPage(ctx, config.ScanID, page, exhausted, config.MaxDomains); err != nil {
			return fmt.Errorf("commit AXFR source page: %w", err)
		}
	}
}

func axfrWorkLoop(ctx context.Context, store *axfrStore, config Config, prober *axfrprobe.AXFRProber) error {
	poolContext, stopPool := context.WithCancel(ctx)
	defer stopPool()
	jobs := make(chan probeJob, config.ClaimBatch)
	results := make(chan struct {
		job     probeJob
		outcome axfrprobe.AXFROutcome
	}, config.ClaimBatch)
	workerGroup, workerContext := errgroup.WithContext(poolContext)
	for range config.Workers {
		workerGroup.Go(func() error {
			for job := range jobs {
				outcome := prober.ProbeServer(workerContext, job.RootDomain, job.NameServer, job.NameServerIP)
				select {
				case results <- struct {
					job     probeJob
					outcome axfrprobe.AXFROutcome
				}{job: job, outcome: outcome}:
				case <-workerContext.Done():
					return workerContext.Err()
				}
			}
			return nil
		})
	}
	workersDone := make(chan error, 1)
	go func() { workersDone <- workerGroup.Wait() }()

	inFlight := 0
	refillAt := max(1, config.ClaimBatch/2)
	for {
		if inFlight <= refillAt {
			claimed, err := store.claim(ctx, config.ScanID, config.ClaimBatch-inFlight)
			if err != nil {
				close(jobs)
				return fmt.Errorf("claim AXFR endpoints: %w", err)
			}
			for _, job := range claimed {
				select {
				case jobs <- job:
					inFlight++
				case <-ctx.Done():
					close(jobs)
					return ctx.Err()
				}
			}
			if len(claimed) == 0 && inFlight == 0 {
				done, err := axfrProductionDone(ctx, store, config.ScanID)
				if err != nil {
					close(jobs)
					return err
				}
				if done {
					close(jobs)
					return <-workersDone
				}
				if err := axfrWait(ctx, axfrIdlePollInterval); err != nil {
					close(jobs)
					return err
				}
				continue
			}
		}

		select {
		case result := <-results:
			if err := store.commit(ctx, config.ScanID, result.job, result.outcome); err != nil {
				close(jobs)
				return fmt.Errorf("commit AXFR endpoint %s/%s: %w", result.job.RootDomain, result.job.NameServerIP, err)
			}
			inFlight--
		case err := <-workersDone:
			close(jobs)
			if err == nil && inFlight > 0 {
				return fmt.Errorf("AXFR probe pool stopped with %d endpoints in flight", inFlight)
			}
			return err
		case <-ctx.Done():
			close(jobs)
			return ctx.Err()
		}
	}
}

func axfrFlushLoop(ctx context.Context, store *axfrStore, config Config) error {
	for {
		connection, err := clickhouseconn.Open()
		if err == nil {
			loaded, flushErr := flushReady(ctx, connection, store, config.ScanID, config.FlushBatch)
			_ = connection.Close()
			if flushErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				slog.Error("AXFR flush failed", "scan_id", config.ScanID, "error", flushErr)
			} else if loaded > 0 {
				continue
			}
		} else {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("open AXFR output connection", "scan_id", config.ScanID, "error", err)
		}
		done, err := axfrDrainDone(ctx, store, config.ScanID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if err := axfrWait(ctx, config.FlushInterval); err != nil {
			return err
		}
	}
}

func axfrStatsLoop(ctx context.Context, store *axfrStore, config Config, startedAt time.Time) error {
	previous, err := store.stats(ctx, config.ScanID)
	if err != nil {
		return fmt.Errorf("read initial AXFR stats: %w", err)
	}
	previousAt := time.Now().UTC()
	for {
		if err := axfrWait(ctx, config.StatsInterval); err != nil {
			return err
		}
		current, err := store.stats(ctx, config.ScanID)
		if err != nil {
			return fmt.Errorf("read AXFR stats: %w", err)
		}
		now := time.Now().UTC()
		elapsed := now.Sub(startedAt).Seconds()
		interval := now.Sub(previousAt).Seconds()
		speed, average := 0.0, 0.0
		if interval > 0 {
			speed = float64(current.Tried-previous.Tried) / interval
		}
		if elapsed > 0 {
			average = float64(current.Tried) / elapsed
		}
		slog.Info("stats", "component", "axfr", "successful", current.Successful,
			"tried", current.Tried, "open", current.Open, "closed", current.Closed,
			"unknown", current.Unknown, "speed", fmt.Sprintf("%.1f probes/s", speed),
			"average", fmt.Sprintf("%.1f probes/s", average), "scan_id", config.ScanID)
		previous = current
		previousAt = now
		done, err := axfrDrainDone(ctx, store, config.ScanID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func axfrProductionDone(ctx context.Context, store *axfrStore, scanID string) (bool, error) {
	state, err := store.state(ctx, scanID)
	if err != nil {
		return false, err
	}
	remaining, err := store.workRemaining(ctx, scanID)
	return state.SourceExhausted && remaining == 0, err
}

func axfrDrainDone(ctx context.Context, store *axfrStore, scanID string) (bool, error) {
	state, err := store.state(ctx, scanID)
	if err != nil {
		return false, err
	}
	active, err := store.activeDomains(ctx, scanID)
	return state.SourceExhausted && active == 0, err
}

func axfrWait(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
