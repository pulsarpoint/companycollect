package service

import (
	"context"

	pgx "github.com/jackc/pgx/v5"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

// TxPool abstracts *pgxpool.Pool so service tests can inject pgxmock.
type TxPool interface {
	db.DBTX
	Begin(ctx context.Context) (pgx.Tx, error)
}
