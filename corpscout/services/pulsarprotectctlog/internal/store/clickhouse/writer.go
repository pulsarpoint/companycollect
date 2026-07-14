package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

const hostnameInsertSQL = `INSERT INTO %s.hostnames (
		registered_domain, fqdn, is_wildcard, first_seen, last_seen,
		last_not_after, source_logs, last_ingested_at)`

// Store is the ClickHouse data-plane handle for the CT log tables.
type Store struct {
	conn     driver.Conn
	database string
}

// Open connects to ClickHouse using the native protocol. The session connects
// to the always-present "default" database so the target database can be
// created on demand; all table operations are fully qualified with database.
func Open(ctx context.Context, addr, database, user, password string) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: user,
			Password: password,
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &Store{conn: conn, database: database}, nil
}

// Close releases the underlying connection.
func (s *Store) Close() error { return s.conn.Close() }

// WriteCerts inserts certificate metadata rows in a single batch.
func (s *Store) WriteCerts(ctx context.Context, certs []model.CertMeta) error {
	if len(certs) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(`INSERT INTO %s.certs (
		issuer_ca_id, serial_number, fingerprint_sha256, common_name, sans,
		issuer_name, not_before, not_after, sct_timestamp, log_name, log_index,
		entry_type, signature_algorithm, public_key_algorithm, key_size,
		is_ca, is_wildcard)`, s.database))
	if err != nil {
		return fmt.Errorf("prepare certs batch: %w", err)
	}
	for _, c := range certs {
		if err := batch.Append(
			c.IssuerCAID,
			c.SerialNumber,
			c.FingerprintSHA256,
			c.CommonName,
			c.SANs,
			c.IssuerName,
			c.NotBefore,
			c.NotAfter,
			c.SCTTimestamp,
			c.LogName,
			c.LogIndex,
			c.EntryType.String(),
			c.SignatureAlgorithm,
			c.PublicKeyAlgorithm,
			uint16(c.KeySize),
			boolToUint8(c.IsCA),
			boolToUint8(c.IsWildcard),
		); err != nil {
			return fmt.Errorf("append cert row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send certs batch: %w", err)
	}
	return nil
}

// WriteHostnames inserts distinct-hostname observations in a single batch. Each
// row seeds the SimpleAggregateFunction columns with plain values; the
// AggregatingMergeTree folds repeated observations of the same hostname into one
// row on merge. source_logs is written as a single-element array that
// groupUniqArrayArray unions across observations.
func (s *Store) WriteHostnames(ctx context.Context, rows []model.HostnameRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(hostnameInsertSQL, s.database))
	if err != nil {
		return fmt.Errorf("prepare hostnames batch: %w", err)
	}
	if err := appendHostnameRows(batch, rows, time.Now().UTC()); err != nil {
		return err
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send hostnames batch: %w", err)
	}
	return nil
}

func appendHostnameRows(batch driver.Batch, rows []model.HostnameRow, ingestedAt time.Time) error {
	for _, r := range rows {
		if err := batch.Append(
			r.RegisteredDomain,
			r.FQDN,
			boolToUint8(r.IsWildcard),
			r.FirstSeen,
			r.LastSeen,
			r.LastNotAfter,
			[]string{r.SourceLog},
			ingestedAt,
		); err != nil {
			return fmt.Errorf("append hostname row: %w", err)
		}
	}
	return nil
}

func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
