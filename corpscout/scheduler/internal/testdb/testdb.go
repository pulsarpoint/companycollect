package testdb

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const testDatabaseURLEnv = "CORPSCOUT_TEST_DATABASE_URL"

func BeginTx(t *testing.T) pgx.Tx {
	t.Helper()
	dsn := os.Getenv(testDatabaseURLEnv)
	if dsn == "" {
		t.Skipf("%s is not set", testDatabaseURLEnv)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	var rawRecordsRegclass *string
	err = pool.QueryRow(ctx, "SELECT to_regclass('brreg_workflow.raw_records')::text").Scan(&rawRecordsRegclass)
	require.NoError(t, err)
	if rawRecordsRegclass == nil {
		t.Fatalf("%s must point to a migrated Corpscout test database", testDatabaseURLEnv)
	}

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	return tx
}
