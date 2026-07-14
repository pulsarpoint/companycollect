// Package clickhouse holds the CT log data-plane schema and batched writer.
package clickhouse

import (
	"context"
	"fmt"
)

// certsDDL is the per-certificate metadata table. Deduplicated by
// (issuer_ca_id, serial_number) so precert/final-cert pairs and cross-log
// duplicates collapse under ReplacingMergeTree merges. Partitioned by expiry
// month and bounded by a rolling TTL: every not-yet-expired cert is retained,
// plus a 90-day grace tail past expiry, after which whole partitions drop. It
// is a windowed store for expiration tracking and on-demand cert lookups, not a
// permanent archive.
const certsDDL = `CREATE TABLE IF NOT EXISTS %s.certs (
  issuer_ca_id        String,
  serial_number       String,
  fingerprint_sha256  String,
  common_name         String,
  sans                Array(String),
  issuer_name         String,
  not_before          DateTime,
  not_after           DateTime,
  sct_timestamp       DateTime64(3),
  log_name            LowCardinality(String),
  log_index           UInt64,
  entry_type          Enum8('unknown'=0,'precert'=1,'cert'=2),
  signature_algorithm LowCardinality(String),
  public_key_algorithm LowCardinality(String),
  key_size            UInt16,
  is_ca               UInt8,
  is_wildcard         UInt8,
  inserted_at         DateTime DEFAULT now(),
  INDEX idx_sans sans TYPE bloom_filter GRANULARITY 4,
  INDEX idx_notafter not_after TYPE minmax GRANULARITY 4
) ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(not_after)
ORDER BY (issuer_ca_id, serial_number)
TTL not_after + INTERVAL 90 DAY DELETE%s`

// hostnamesDDL is the distinct-hostname store and the primary query surface.
// One logical row per (registered_domain, fqdn), permanent (no TTL). Written
// for every certificate; repeated observations collapse under AggregatingMergeTree
// merges into min(first_seen)/max(last_seen). max(last_ingested_at) is a separate
// writer-time watermark for incremental consumers; legacy rows use the Unix epoch
// sentinel. The SimpleAggregateFunction columns accept plain values on INSERT and
// expose the merged value directly, but reads must still fold unmerged parts with
// GROUP BY (or FINAL). The partition key is a stable hash of registered_domain
// (constant for a given fqdn) so every observation of a hostname lands in one
// partition — required for merge-based dedup and a bonus for registered_domain=
// query pruning.
const hostnamesDDL = `CREATE TABLE IF NOT EXISTS %s.hostnames (
  registered_domain  String,
  fqdn               String,
  is_wildcard        SimpleAggregateFunction(max, UInt8),
  first_seen         SimpleAggregateFunction(min, DateTime64(3)),
  last_seen          SimpleAggregateFunction(max, DateTime64(3)),
  last_ingested_at  SimpleAggregateFunction(max, DateTime64(3, 'UTC')) DEFAULT toDateTime64(0, 3, 'UTC'),
  last_not_after     SimpleAggregateFunction(max, DateTime),
  source_logs        SimpleAggregateFunction(groupUniqArrayArray, Array(String))
) ENGINE = AggregatingMergeTree
PARTITION BY cityHash64(registered_domain) %% 16
ORDER BY (registered_domain, fqdn)%s`

// EnsureSchema creates the database and tables if they do not exist. If
// storagePolicy is non-empty, both tables are pinned to it for disk isolation.
func (s *Store) EnsureSchema(ctx context.Context, storagePolicy string) error {
	if err := s.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", s.database)); err != nil {
		return fmt.Errorf("create database %s: %w", s.database, err)
	}
	settings := ""
	if storagePolicy != "" {
		settings = fmt.Sprintf("\nSETTINGS storage_policy = '%s'", storagePolicy)
	}
	if err := s.conn.Exec(ctx, fmt.Sprintf(certsDDL, s.database, settings)); err != nil {
		return fmt.Errorf("create certs table: %w", err)
	}
	if err := s.conn.Exec(ctx, fmt.Sprintf(hostnamesDDL, s.database, settings)); err != nil {
		return fmt.Errorf("create hostnames table: %w", err)
	}
	return nil
}
