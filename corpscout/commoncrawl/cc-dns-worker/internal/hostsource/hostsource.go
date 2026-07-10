// Package hostsource loads discovered subdomain labels for a batch of domains from two ClickHouse
// sources — Certificate Transparency (ctlogs.hostnames) and the durable registry
// (commoncrawl_domain_hostnames) — scoped to exactly the domains one scan seeded (Task 11), and streams
// the union/rank/cap result computed server-side into the caller's local stage.
package hostsource

import (
	"context"
	"strconv"
	"strings"

	"cc-dns-worker/internal/model"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// NormalizeLabel turns an fqdn into a scan label relative to rootDomain: strips the ".<rootDomain>"
// suffix and lowercases. Returns ok=false for the apex, non-subdomains, wildcards, or an empty label.
// This is the Go-side reference for what shardQuery's label-extraction expression computes in SQL —
// kept (and tested) so the two never silently drift apart, even though production label extraction now
// happens entirely in ClickHouse (see shardQuery's doc comment).
func NormalizeLabel(rootDomain, fqdn string) (string, bool) {
	fqdn = strings.ToLower(strings.TrimSuffix(fqdn, "."))
	rootDomain = strings.ToLower(rootDomain)
	suffix := "." + rootDomain
	if fqdn == rootDomain || !strings.HasSuffix(fqdn, suffix) {
		return "", false
	}
	label := strings.TrimSuffix(fqdn, suffix)
	if label == "" || strings.Contains(label, "*") {
		return "", false
	}
	return label, true
}

// CTPartitions is the number of hash shards the seed-time host-load splits domains into. It MUST equal
// the ctlogs.hostnames physical partitioning (PARTITION BY cityHash64(registered_domain) % 16) so each
// shard's `cityHash64(registered_domain) % 16 = N` filter prunes to exactly one physical partition —
// the whole point of the sharding. Changing it without repartitioning ctlogs defeats partition pruning.
const CTPartitions = 16

// Source precedence, encoded as an EXPLICIT numeric priority (Task 11) — never a lexical string
// comparison (the bug this replaces: the old RegistryShard used `min(discovery_source)`, which only
// happened to rank axfr < ct < static because those particular strings sort that way alphabetically).
// Higher wins: axfrPriority > ctPriority > staticPriority. shardQuery's ORDER BY sorts on this number
// descending, so every axfr-sourced label sorts ahead of every ct-sourced label regardless of how many
// of either exist — which is exactly what makes the per-domain cap unable to let CT evict a durable
// AXFR label (see shardQuery's doc comment).
const (
	axfrPriority   = 3
	ctPriority     = 2
	staticPriority = 1
)

// shardQuery builds the scan-scoped, ranked, capped CT+registry union query for hash-shard `shard` of
// `numShards` (see CTPartitions). It takes scanID twice as `?` bind args (once per branch) — the driver
// substitutes and quotes it safely, so this is not string-interpolated into the SQL.
//
// Task 11 fixes four bugs in the query itself, not just in Go:
//
//  1. Scope: BOTH the CT branch and the registry branch join to corpscout.dns_scan_seed_domains WHERE
//     scan_id = ?, so only domains this scan actually seeded are ever considered — never the whole
//     commoncrawl_domains corpus (the old CTShard/RegistryShard semi-join target).
//  2. Union/rank/cap all happen in ClickHouse: the two branches (ctBranch/regBranch, unioned) are
//     deduped by (root_domain, label) via one GROUP BY, ranked by (priority DESC, live DESC, recency
//     DESC), and THEN capped via `LIMIT capN BY root_domain` — applied once, to the final merged set,
//     not per-source. This is what makes eviction deterministic: the old client-side Merge capped
//     AFTER concatenating CT-then-registry slices in a fixed, source-biased order, which could silently
//     drop an AXFR label appended after the cap already filled up on CT entries.
//  3. Priority is an explicit number (axfrPriority/ctPriority/staticPriority), computed via multiIf —
//     see the package doc comment on those constants.
//  4. Because AXFR (priority 3) always sorts ahead of CT (priority 2) in the ORDER BY that `LIMIT n BY
//     root_domain` respects, a domain with MORE than capN CT-only labels can never push out a durable
//     AXFR label: every AXFR row for that domain is already positioned before every CT row in the sort,
//     so the cap only ever trims from the bottom (lowest priority, then least live, then least recent).
//
// The CT branch also performs the fqdn->label normalization inline in SQL (mirroring NormalizeLabel):
// lower-cases and strips a trailing dot, requires fqdn to be a proper subdomain of registered_domain
// (never the apex), and drops any label containing '*' — the same rules NormalizeLabel encodes in Go
// (kept for reference/tests; production label extraction is now this SQL, not that function). Live-cert
// wildcards are already excluded upstream via is_wildcard=0.
func shardQuery(shard, numShards, capN int) string {
	ns, sh, cp := strconv.Itoa(numShards), strconv.Itoa(shard), strconv.Itoa(capN)
	ap, ctp, sp := strconv.Itoa(axfrPriority), strconv.Itoa(ctPriority), strconv.Itoa(staticPriority)

	ctBranch := `
        SELECT root_domain, label, ` + ctp + ` AS priority, live, recency
        FROM (
            SELECT registered_domain AS root_domain, label, live, ls AS recency
            FROM (
                SELECT registered_domain, ls, (lna >= now()) AS live,
                       if(fqdn_norm != rd_norm AND endsWith(fqdn_norm, concat('.', rd_norm)),
                          substring(fqdn_norm, 1, length(fqdn_norm) - length(rd_norm) - 1), '') AS label
                FROM (
                    SELECT registered_domain, ls, lna,
                           lower(trimRight(fqdn, '.')) AS fqdn_norm,
                           lower(registered_domain) AS rd_norm
                    FROM (
                        SELECT registered_domain, fqdn,
                               max(is_wildcard)    AS is_wc,
                               max(last_seen)      AS ls,
                               max(last_not_after) AS lna
                        FROM ctlogs.hostnames
                        WHERE cityHash64(registered_domain) % ` + ns + ` = ` + sh + `
                          AND registered_domain IN (
                              SELECT root_domain FROM corpscout.dns_scan_seed_domains
                              WHERE scan_id = ? AND cityHash64(root_domain) % ` + ns + ` = ` + sh + `
                          )
                        GROUP BY registered_domain, fqdn
                    )
                    WHERE is_wc = 0
                )
            )
            WHERE label != '' AND label NOT LIKE '%*%'
        )`

	regBranch := `
        SELECT root_domain, label, priority, 0 AS live, recency
        FROM (
            SELECT root_domain, label,
                   max(multiIf(discovery_source = 'axfr', ` + ap + `, discovery_source = 'ct', ` + ctp + `, discovery_source = 'static', ` + sp + `, 0)) AS priority,
                   max(last_seen) AS recency
            FROM corpscout.commoncrawl_domain_hostnames
            WHERE cityHash64(root_domain) % ` + ns + ` = ` + sh + `
              AND root_domain IN (
                  SELECT root_domain FROM corpscout.dns_scan_seed_domains
                  WHERE scan_id = ? AND cityHash64(root_domain) % ` + ns + ` = ` + sh + `
              )
            GROUP BY root_domain, label
        )`

	return `
SELECT root_domain, label,
       multiIf(top_priority = ` + ap + `, 'axfr', top_priority = ` + ctp + `, 'ct', top_priority = ` + sp + `, 'static', 'unknown') AS source,
       top_live
FROM (
    SELECT root_domain, label,
           max(priority) AS top_priority,
           max(live)     AS top_live,
           max(recency)  AS top_recency
    FROM (` + ctBranch + `

        UNION ALL
` + regBranch + `
    )
    GROUP BY root_domain, label
)
ORDER BY root_domain, top_priority DESC, top_live DESC, top_recency DESC
LIMIT ` + cp + ` BY root_domain`
}

// ShardStream runs shardQuery for hash-shard `shard` of `numShards` against scanID's seeded membership
// and invokes fn once per bounded batch of up to batchSize rows. The union, dedupe, priority-rank, and
// per-domain cap all happen in ClickHouse (see shardQuery) — Go never materializes a whole shard's
// result as a map[string][]HostLabel (Task 11 — the bug this replaces: CTShard/RegistryShard/Merge, all
// removed). fn must consume the batch before returning: like input.StreamClickHouse, the backing slice
// is reused across calls.
func ShardStream(ctx context.Context, conn driver.Conn, scanID string, shard, numShards, capN, batchSize int, fn func([]model.ScanHostnameRow) error) error {
	rows, err := conn.Query(ctx, shardQuery(shard, numShards, capN), scanID, scanID)
	if err != nil {
		return err
	}
	defer rows.Close()
	return drainShardRows(rows.Next, func(r *model.ScanHostnameRow) error {
		var live uint8
		if err := rows.Scan(&r.RootDomain, &r.Label, &r.DiscoverySource, &live); err != nil {
			return err
		}
		r.LiveCert = live != 0
		return nil
	}, rows.Err, batchSize, fn)
}

// drainShardRows is the ClickHouse-free batching core of ShardStream (mirrors internal/input's
// drainRows): it pulls rows via next()/scan, groups them into batches of batchSize, and calls fn per
// full batch plus a final partial batch — never holding more than one batch in memory at a time.
// Exercised directly in tests with a synthetic next/scan pair, so the bounded-memory property is
// provable without a real ClickHouse connection.
func drainShardRows(next func() bool, scan func(*model.ScanHostnameRow) error, rowsErr func() error, batchSize int, fn func([]model.ScanHostnameRow) error) error {
	if batchSize <= 0 {
		batchSize = 5000
	}
	batch := make([]model.ScanHostnameRow, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := fn(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	for next() {
		var r model.ScanHostnameRow
		if err := scan(&r); err != nil {
			return err
		}
		batch = append(batch, r)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rowsErr(); err != nil {
		return err
	}
	return flush()
}
