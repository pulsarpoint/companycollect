package cvrdb

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type TxPool interface {
	db.DBTX
	Begin(context.Context) (pgx.Tx, error)
}

type Gateway struct {
	pool TxPool
}

func New(pool TxPool) *Gateway {
	return &Gateway{pool: pool}
}

func (g *Gateway) withTx(ctx context.Context, fn func(*db.Queries) error) error {
	if g == nil || g.pool == nil {
		return errors.New("cvr workflow database pool not available")
	}
	tx, err := g.pool.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "begin cvr workflow transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(db.New(tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.Wrap(err, "commit cvr workflow transaction")
	}
	return nil
}
