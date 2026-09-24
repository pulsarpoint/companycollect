// Package store owns the bounded, restartable DNS SQLite work queue and output outbox.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"cc-dns-scan/internal/model"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	database, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open SQLite: %w", err)
	}
	database.SetMaxOpenConns(1)
	if _, err := database.ExecContext(context.Background(), boundedSchema); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("create bounded schema: %w", err)
	}
	if err := upgradeRecordOutboxes(context.Background(), database); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("upgrade bounded record outboxes: %w", err)
	}
	return &Store{db: database}, nil
}

type sqliteColumn struct {
	name       string
	definition string
}

// upgradeRecordOutboxes preserves queued records created by an older worker while adding the
// protocol metadata needed for universal RR storage. Existing rows retain explicit zero/empty
// values, meaning that the old worker did not capture those fields.
func upgradeRecordOutboxes(ctx context.Context, database *sql.DB) error {
	columns := []sqliteColumn{
		{"record_type_code", "INTEGER NOT NULL DEFAULT 0"},
		{"record_class_code", "INTEGER NOT NULL DEFAULT 0"},
		{"rdata_wire", "BLOB NOT NULL DEFAULT X''"},
		{"name_server", "TEXT NOT NULL DEFAULT ''"},
		{"name_server_ip", "TEXT NOT NULL DEFAULT ''"},
	}
	for _, table := range []string{"dns_records"} {
		existing, err := sqliteColumns(ctx, database, table)
		if err != nil {
			return err
		}
		for _, column := range columns {
			if existing[column.name] {
				continue
			}
			if _, err := database.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+column.name+" "+column.definition); err != nil {
				return fmt.Errorf("add %s.%s: %w", table, column.name, err)
			}
		}
	}
	stateColumns := []sqliteColumn{
		{"domains_processed", "INTEGER NOT NULL DEFAULT 0"},
		{"records_observed", "INTEGER NOT NULL DEFAULT 0"},
		{"dns_checks", "INTEGER NOT NULL DEFAULT 0"},
		{"dns_checks_ok", "INTEGER NOT NULL DEFAULT 0"},
	}
	existing, err := sqliteColumns(ctx, database, "scan_state")
	if err != nil {
		return err
	}
	for _, column := range stateColumns {
		if existing[column.name] {
			continue
		}
		if _, err := database.ExecContext(ctx, "ALTER TABLE scan_state ADD COLUMN "+column.name+" "+column.definition); err != nil {
			return fmt.Errorf("add scan_state.%s: %w", column.name, err)
		}
	}
	return nil
}

func sqliteColumns(ctx context.Context, database *sql.DB, table string) (map[string]bool, error) {
	rows, err := database.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return nil, fmt.Errorf("inspect %s columns: %w", table, err)
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, fmt.Errorf("read %s columns: %w", table, err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read %s columns: %w", table, err)
	}
	return columns, nil
}

func (s *Store) Close() error { return s.db.Close() }

func parseTS(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}

func b2i(value bool) int {
	if value {
		return 1
	}
	return 0
}

func setScanRowEndpoints(row *model.ScanRow) {
	for _, endpoint := range row.Endpoints {
		row.NSEndpointNames = append(row.NSEndpointNames, endpoint.Name)
		row.NSEndpointIPs = append(row.NSEndpointIPs, endpoint.IP)
		row.NSEndpointScopes = append(row.NSEndpointScopes, endpoint.Scope)
		row.NSEndpointDialable = append(row.NSEndpointDialable, uint8(b2i(endpoint.Dialable)))
	}
}
