package hostsource

import (
	"strings"
	"testing"
)

func TestBatchQueryReadsOnlyConfirmedHostnameView(t *testing.T) {
	if !strings.Contains(batchQuery, "FROM corpscout.domain_hostnames") {
		t.Fatalf("query does not read the confirmed hostname view: %s", batchQuery)
	}
	for _, forbidden := range []string{
		"commoncrawl_domain_hostnames",
		"commoncrawl_domain_dns_record_observations",
		"ctlogs.hostnames",
		"commoncrawl_domains",
	} {
		if strings.Contains(batchQuery, forbidden) {
			t.Errorf("query contains removed source %q: %s", forbidden, batchQuery)
		}
	}
}

func TestBatchQueryBindsRootsAndCap(t *testing.T) {
	if !strings.Contains(batchQuery, "WHERE root_domain IN (?)") {
		t.Errorf("root batch must use a bound primary-key filter: %s", batchQuery)
	}
	if !strings.Contains(batchQuery, "LIMIT ? BY root_domain") {
		t.Errorf("per-root cap must be a bound parameter: %s", batchQuery)
	}
}

func TestBatchQueryRanksProvenanceThenObservationRecency(t *testing.T) {
	wantPriority := "discovery_source = 'axfr', 3, discovery_source = 'ct', 2"
	if !strings.Contains(batchQuery, wantPriority) {
		t.Errorf("query does not encode AXFR > CT priority: %s", batchQuery)
	}
	if strings.Contains(batchQuery, "last_not_after") || strings.Contains(batchQuery, "live_cert") {
		t.Errorf("DNS-only view query must not use CT certificate state: %s", batchQuery)
	}
	if !strings.Contains(batchQuery, "priority DESC, last_seen DESC") {
		t.Errorf("query has the wrong rank order: %s", batchQuery)
	}
}

func TestBatchQueryPreservesMultiLabelSubdomains(t *testing.T) {
	if strings.Contains(batchQuery, "splitByChar") || strings.Contains(batchQuery, "substring") {
		t.Errorf("view labels are already relative and must not be shortened: %s", batchQuery)
	}
}
