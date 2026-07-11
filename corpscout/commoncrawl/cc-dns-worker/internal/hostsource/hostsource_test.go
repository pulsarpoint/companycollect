package hostsource

import (
	"strings"
	"testing"
)

func TestBatchQueryReadsOnlyDurableRegistry(t *testing.T) {
	if !strings.Contains(batchQuery, "FROM corpscout.commoncrawl_domain_hostnames") {
		t.Fatalf("query does not read the hostname registry: %s", batchQuery)
	}
	for _, forbidden := range []string{"ctlogs.hostnames", "commoncrawl_domains"} {
		if strings.Contains(batchQuery, forbidden) {
			t.Errorf("query contains removed source %q: %s", forbidden, batchQuery)
		}
	}
}

func TestBatchQueryBindsRootsAndCap(t *testing.T) {
	if !strings.Contains(batchQuery, "WHERE has(?, root_domain)") {
		t.Errorf("root batch must be a bound array parameter: %s", batchQuery)
	}
	if !strings.Contains(batchQuery, "LIMIT ? BY root_domain") {
		t.Errorf("per-root cap must be a bound parameter: %s", batchQuery)
	}
}

func TestBatchQueryRanksAXFRThenLiveCT(t *testing.T) {
	wantPriority := "discovery_source = 'axfr', 3, discovery_source = 'ct', 2"
	if !strings.Contains(batchQuery, wantPriority) {
		t.Errorf("query does not encode AXFR > CT priority: %s", batchQuery)
	}
	if !strings.Contains(batchQuery, "last_not_after >= now()") {
		t.Errorf("query does not rank live CT entries: %s", batchQuery)
	}
	if !strings.Contains(batchQuery, "priority DESC, live_cert DESC, recency DESC") {
		t.Errorf("query has the wrong rank order: %s", batchQuery)
	}
}

func TestBatchQueryPreservesMultiLabelSubdomains(t *testing.T) {
	if strings.Contains(batchQuery, "splitByChar") || strings.Contains(batchQuery, "substring") {
		t.Errorf("registry labels are already relative and must not be shortened: %s", batchQuery)
	}
}
