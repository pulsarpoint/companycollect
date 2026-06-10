package app

import (
	"context"
	"log/slog"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/s3client"
)

func setupRiver(ctx context.Context, pool *pgxpool.Pool, q db.Querier, s3 *s3client.Client) (*river.Client[pgx.Tx], error) {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return nil, err
	}
	res, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	if err != nil {
		return nil, err
	}
	for _, v := range res.Versions {
		slog.Info("river migration applied", "version", v.Version, "direction", "up")
	}

	_ = q
	_ = s3

	w, enabled := newRiverWorkers()
	if !enabled {
		slog.Info("river client disabled; no workers registered")
		return nil, nil
	}

	riverCfg := &river.Config{
		Queues: map[string]river.QueueConfig{
			"default": {MaxWorkers: 1},
		},
		Workers: w,
	}

	rc, err := river.NewClient(riverpgxv5.New(pool), riverCfg)
	if err != nil {
		return nil, err
	}
	return rc, nil
}

func newRiverWorkers() (*river.Workers, bool) {
	return nil, false
}
