//go:build integration

package hostsource

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func testConn(t *testing.T) driver.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{envOr("CLICKHOUSE_HOST", "localhost") + ":" + envOr("CLICKHOUSE_NATIVE_PORT", "9000")},
		Auth: clickhouse.Auth{
			Database: envOr("CLICKHOUSE_DATABASE", "corpscout"),
			Username: envOr("CLICKHOUSE_USER", "default"),
			Password: envOr("CLICKHOUSE_PASSWORD", ""),
		},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return conn
}

func insertObservation(
	t *testing.T,
	ctx context.Context,
	conn driver.Conn,
	rootDomain string,
	name string,
	recordType string,
	discovery string,
	scanID string,
	observedAt time.Time,
) {
	t.Helper()
	source := "query"
	if discovery == "axfr" {
		source = "axfr"
	}
	if err := conn.Exec(ctx, `INSERT INTO corpscout.commoncrawl_domain_dns_record_ingest
		(root_domain, name, record_type, value, source, discovery, scan_id, observed_at, loaded_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rootDomain, name, recordType, "integration-test-value", source, discovery, scanID,
		observedAt, time.Now().UTC()); err != nil {
		t.Fatalf("insert DNS observation for %s/%s: %v", rootDomain, name, err)
	}
}

func cleanupObservations(t *testing.T, ctx context.Context, conn driver.Conn, roots []string) {
	t.Helper()
	t.Cleanup(func() {
		for _, rootDomain := range roots {
			_ = conn.Exec(ctx, `DELETE FROM corpscout.commoncrawl_domain_dns_records
				WHERE root_domain = ?`, rootDomain)
			_ = conn.Exec(ctx, `DELETE FROM corpscout.domain_hostnames_state
				WHERE root_domain = ?`, rootDomain)
		}
		_ = conn.Close()
	})
}

func TestFetchReturnsOnlyRequestedRoots(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	suffix := time.Now().UTC().Format("150405000000000")
	requested := []string{"a-" + suffix + ".test", "b-" + suffix + ".test", "c-" + suffix + ".test"}
	unrequested := "d-" + suffix + ".test"
	all := append(append([]string{}, requested...), unrequested)
	cleanupObservations(t, ctx, conn, all)
	for _, rootDomain := range all {
		insertObservation(t, ctx, conn, rootDomain, "www."+rootDomain, "A", "ct", suffix, time.Now().UTC())
	}

	got, err := Fetch(ctx, conn, requested, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, rootDomain := range requested {
		if len(got[rootDomain]) != 1 || got[rootDomain][0].Label != "www" {
			t.Errorf("%s rows = %+v, want one www label", rootDomain, got[rootDomain])
		}
	}
	if _, exists := got[unrequested]; exists {
		t.Errorf("unrequested root leaked into result: %+v", got[unrequested])
	}
}

func TestFetchCapKeepsAXFRBeforeCT(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	suffix := time.Now().UTC().Format("150405000000000")
	rootDomain := "cap-" + suffix + ".test"
	cleanupObservations(t, ctx, conn, []string{rootDomain})
	for index := range 3 {
		label := fmt.Sprintf("axfr-%d", index)
		insertObservation(t, ctx, conn, rootDomain, label+"."+rootDomain, "A", "axfr", suffix, time.Now().UTC())
	}
	for index := range 20 {
		label := fmt.Sprintf("ct-%d", index)
		insertObservation(t, ctx, conn, rootDomain, label+"."+rootDomain, "A", "ct", suffix, time.Now().UTC())
	}

	got, err := Fetch(ctx, conn, []string{rootDomain}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[rootDomain]) != 5 {
		t.Fatalf("rows = %d, want cap of 5", len(got[rootDomain]))
	}
	axfrCount := 0
	for _, hostname := range got[rootDomain] {
		if hostname.DiscoverySource == "axfr" {
			axfrCount++
		}
	}
	if axfrCount != 3 {
		t.Errorf("AXFR rows kept = %d, want all 3: %+v", axfrCount, got[rootDomain])
	}
}

func TestFetchReturnsOnlyConfirmedAddressAndCNAMEOwners(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	suffix := time.Now().UTC().Format("150405000000000")
	rootDomain := "types-" + suffix + ".test"
	cleanupObservations(t, ctx, conn, []string{rootDomain})
	observedAt := time.Now().UTC()

	fixtures := []struct {
		name       string
		recordType string
		discovery  string
	}{
		{name: "www." + rootDomain, recordType: "A", discovery: "static"},
		{name: "mail." + rootDomain, recordType: "AAAA", discovery: "static"},
		{name: "alias." + rootDomain, recordType: "CNAME", discovery: "ct"},
		{name: "multi." + rootDomain, recordType: "A", discovery: "axfr"},
		{name: "multi." + rootDomain, recordType: "AAAA", discovery: "axfr"},
		{name: "mx-only." + rootDomain, recordType: "MX", discovery: "axfr"},
		{name: "txt-only." + rootDomain, recordType: "TXT", discovery: "static"},
		{name: "*." + rootDomain, recordType: "A", discovery: "axfr"},
		{name: rootDomain, recordType: "A", discovery: "static"},
		{name: "outside.example.net", recordType: "A", discovery: "static"},
	}
	for index, fixture := range fixtures {
		insertObservation(t, ctx, conn, rootDomain, fixture.name, fixture.recordType,
			fixture.discovery, fmt.Sprintf("%s-%d", suffix, index), observedAt)
	}

	got, err := Fetch(ctx, conn, []string{rootDomain}, 100)
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]bool{}
	for _, hostname := range got[rootDomain] {
		labels[hostname.Label] = true
	}
	want := map[string]bool{"www": true, "mail": true, "alias": true, "multi": true}
	if len(labels) != len(want) {
		t.Fatalf("labels = %v, want %v", labels, want)
	}
	for label := range want {
		if !labels[label] {
			t.Errorf("confirmed label %q missing from %v", label, labels)
		}
	}
}

func TestIncrementalHostnameStateMergesInsertBlocksAndReplays(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	suffix := time.Now().UTC().Format("150405000000000")
	rootDomain := "state-" + suffix + ".test"
	hostname := "alias." + rootDomain
	cleanupObservations(t, ctx, conn, []string{rootDomain})

	firstSeen := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Millisecond)
	secondSeen := firstSeen.Add(time.Hour)
	lastSeen := firstSeen.Add(2 * time.Hour)
	insertObservation(t, ctx, conn, rootDomain, hostname, "A", "static", suffix+"-a", firstSeen)
	insertObservation(t, ctx, conn, rootDomain, hostname, "AAAA", "ct", suffix+"-aaaa", secondSeen)
	insertObservation(t, ctx, conn, rootDomain, hostname, "CNAME", "axfr", suffix+"-cname", lastSeen)
	insertObservation(t, ctx, conn, rootDomain, hostname, "A", "static", suffix+"-a", firstSeen)

	rows, err := conn.Query(ctx, `SELECT
		has_ipv4,
		has_ipv6,
		has_cname,
		discovery_source,
		first_seen,
		last_seen
	FROM corpscout.domain_hostnames
	WHERE root_domain = ? AND hostname = ?`, rootDomain, hostname)
	if err != nil {
		t.Fatalf("query incremental hostname state: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("incremental hostname state returned no row")
	}
	var hasIPv4, hasIPv6, hasCNAME uint8
	var discoverySource string
	var gotFirstSeen, gotLastSeen time.Time
	if err := rows.Scan(
		&hasIPv4,
		&hasIPv6,
		&hasCNAME,
		&discoverySource,
		&gotFirstSeen,
		&gotLastSeen,
	); err != nil {
		t.Fatalf("scan incremental hostname state: %v", err)
	}
	if rows.Next() {
		t.Fatal("incremental hostname state returned duplicate public rows")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read incremental hostname state: %v", err)
	}

	if hasIPv4 != 1 || hasIPv6 != 1 || hasCNAME != 1 {
		t.Errorf("record flags = (%d, %d, %d), want all 1", hasIPv4, hasIPv6, hasCNAME)
	}
	if discoverySource != "axfr" {
		t.Errorf("discovery source = %q, want axfr", discoverySource)
	}
	if !gotFirstSeen.Equal(firstSeen) {
		t.Errorf("first seen = %s, want %s", gotFirstSeen, firstSeen)
	}
	if !gotLastSeen.Equal(lastSeen) {
		t.Errorf("last seen = %s, want %s", gotLastSeen, lastSeen)
	}

	got, err := Fetch(ctx, conn, []string{rootDomain}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[rootDomain]) != 1 || got[rootDomain][0].Label != "alias" {
		t.Errorf("fetched labels = %+v, want one alias label", got[rootDomain])
	}
}
