package resolve

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"cc-dns-worker/internal/metrics"
	"cc-dns-worker/internal/records"

	"github.com/miekg/dns"
)

type stubEx struct{ z map[string][]dns.RR }

func (s stubEx) Exchange(_ context.Context, m *dns.Msg, _ string) (*dns.Msg, error) {
	r := new(dns.Msg)
	r.SetReply(m)
	q := m.Question[0]
	r.Answer = append(r.Answer, s.z[q.Name+"/"+dns.TypeToString[q.Qtype]]...)
	return r, nil
}

func TestResolveProducesRecords(t *testing.T) {
	z := map[string][]dns.RR{
		"example.com./MX":                    {mustRR(t, "example.com. 300 IN MX 10 mail.example.com.")},
		"example.com./TXT":                   {mustRR(t, `example.com. 300 IN TXT "v=spf1 include:_spf.example.com ~all"`)},
		"_dmarc.example.com./TXT":            {mustRR(t, `_dmarc.example.com. 300 IN TXT "v=DMARC1; p=reject"`)},
		"www.example.com./A":                 {mustRR(t, "www.example.com. 300 IN A 1.2.3.4")},
		"google._domainkey.example.com./TXT": {mustRR(t, `google._domainkey.example.com. 300 IN TXT "v=DKIM1; k=rsa; p=MII"`)},
	}
	r := &Resolver{Ex: stubEx{z: z}}
	del := Delegation{ETLD: "com", NS: []string{"ns1.example.com."}, NSIPs: []string{"9.9.9.9"}, DialableNSIPs: []string{"9.9.9.9"}}

	res := r.Resolve(context.Background(), "example.com", "2026-07-05", "run1", del, records.DefaultConfig(), time.Unix(0, 0).UTC(), nil)

	if res.RootDomain != "example.com" || res.ScanID != "2026-07-05" || res.Status != "done" {
		t.Fatalf("identity/status wrong: %+v", res)
	}
	find := func(rt, slot, wantVal string) bool {
		for _, rec := range res.Records {
			if rec.RecordType == rt && rec.Slot == slot {
				if wantVal == "" || rec.Value == wantVal {
					return true
				}
			}
		}
		return false
	}
	if !find("MX", "", "") {
		t.Errorf("missing MX record; records=%+v", res.Records)
	}
	if !find("TXT", "", "") {
		t.Errorf("missing apex TXT (SPF)")
	}
	if !find("TXT", "dmarc", "") {
		t.Errorf("missing DMARC")
	}
	if !find("A", "www", "1.2.3.4") {
		t.Errorf("missing www A")
	}
	if !find("TXT", "google", "") {
		t.Errorf("missing DKIM google")
	}
	// MX preference is captured in both the priority column and the value (so the ReplacingMergeTree
	// sort key, which includes value but not priority, never collapses two MX at different prefs).
	if !find("MX", "", "10 mail.example.com.") {
		t.Errorf("MX value should be complete presentation rdata; records=%+v", res.Records)
	}
	for _, rec := range res.Records {
		if rec.RecordType == "MX" && rec.Priority != 10 {
			t.Errorf("MX priority = %d, want 10", rec.Priority)
		}
	}
}

// TestCollectCapturesCNAME covers a hostname (e.g. www/mail/autodiscover) that is a CNAME rather
// than an A/AAAA record — common for CDNs, M365, and Google Workspace. Before this fix, collect()
// silently dropped the RR via the switch's default case, losing the hostname entirely.
func TestCollectCapturesCNAME(t *testing.T) {
	z := map[string][]dns.RR{
		"www.example.com./A": {mustRR(t, "www.example.com. 300 IN CNAME foo.cdn.net.")},
	}
	r := &Resolver{Ex: stubEx{z: z}}
	del := Delegation{ETLD: "com", NS: []string{"ns1.example.com."}, NSIPs: []string{"9.9.9.9"}, DialableNSIPs: []string{"9.9.9.9"}}

	res := r.Resolve(context.Background(), "example.com", "2026-07-05", "run1", del, records.DefaultConfig(), time.Unix(0, 0).UTC(), nil)

	found := false
	for _, rec := range res.Records {
		if rec.RecordType == "CNAME" && rec.Slot == "www" && rec.Value == "foo.cdn.net." {
			found = true
		}
	}
	if !found {
		t.Errorf("missing CNAME record for www (slot=www, value=foo.cdn.net); records=%+v", res.Records)
	}
}

func TestCollectUsesAnswerOwnerAndRetainsUnknownType(t *testing.T) {
	query := records.Query{Name: "alias.example.com.", Type: dns.TypeA, Slot: "alias", Discovery: "ct"}
	response := new(dns.Msg)
	response.Answer = []dns.RR{
		mustRR(t, "target.example.com. 60 IN A 192.0.2.1"),
		mustRR(t, `unknown.example.com. 60 IN TYPE65400 \# 4 DEADBEEF`),
	}

	records := collect(query, response, "NOERROR")
	if len(records) != 2 {
		t.Fatalf("collected %d records, want 2", len(records))
	}
	if records[0].Name != "target.example.com" {
		t.Errorf("answer owner = %q, want target.example.com", records[0].Name)
	}
	unknown := records[1]
	if unknown.RecordType != "TYPE65400" || unknown.TypeCode != 65400 || unknown.ClassCode != dns.ClassINET {
		t.Errorf("unknown RR identity lost: %+v", unknown)
	}
	if unknown.Value != `\# 4 DEADBEEF` || !bytes.Equal([]byte(unknown.RDataWire), []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("unknown RR payload lost: value=%q wire=%x", unknown.Value, unknown.RDataWire)
	}
}

// TestCollectCapturesSRVAndHTTPS covers the service-discovery additions: an SRV answer (tagged by
// its service slot, with the priority captured) and an apex HTTPS/SVCB answer.
func TestCollectCapturesSRVAndHTTPS(t *testing.T) {
	z := map[string][]dns.RR{
		"_autodiscover._tcp.example.com./SRV": {mustRR(t, "_autodiscover._tcp.example.com. 300 IN SRV 0 5 443 autodiscover.outlook.com.")},
		"example.com./HTTPS":                  {mustRR(t, `example.com. 300 IN HTTPS 1 . alpn="h2,h3"`)},
	}
	r := &Resolver{Ex: stubEx{z: z}}
	del := Delegation{ETLD: "com", NS: []string{"ns1.example.com."}, NSIPs: []string{"9.9.9.9"}, DialableNSIPs: []string{"9.9.9.9"}}
	res := r.Resolve(context.Background(), "example.com", "sc", "run", del, records.DefaultConfig(), time.Unix(0, 0).UTC(), nil)

	var srv, https bool
	for _, rec := range res.Records {
		if rec.RecordType == "SRV" && rec.Slot == "_autodiscover._tcp" {
			srv = true
			if rec.Priority != 0 || rec.Value == "" {
				t.Errorf("SRV record wrong: %+v", rec)
			}
		}
		if rec.RecordType == "HTTPS" && rec.Slot == "@" && rec.Value != "" {
			https = true
		}
	}
	if !srv {
		t.Errorf("missing SRV record; records=%+v", res.Records)
	}
	if !https {
		t.Errorf("missing HTTPS record; records=%+v", res.Records)
	}
}

// recordingEx wraps stubEx's zone-answer behavior but also records every server address it was asked
// to Exchange with, so a test can assert on what was actually dialed (not just what came back). Safe
// for concurrent use since Resolve fires the plan's queries concurrently.
type recordingEx struct {
	mu     sync.Mutex
	z      map[string][]dns.RR
	dialed []string
}

func (r *recordingEx) Exchange(_ context.Context, m *dns.Msg, server string) (*dns.Msg, error) {
	r.mu.Lock()
	r.dialed = append(r.dialed, server)
	r.mu.Unlock()
	resp := new(dns.Msg)
	resp.SetReply(m)
	q := m.Question[0]
	resp.Answer = append(resp.Answer, r.z[q.Name+"/"+dns.TypeToString[q.Qtype]]...)
	return resp, nil
}

// TestQueryAuthNeverDialsNonPublicServer is the query.go defense-in-depth test: even when queryAuth is
// handed a server list containing a non-public address directly (bypassing DiscoverNS's own filtering —
// simulating a future caller that forgets to pass DialableNSIPs), it must never call Exchange with that
// address, and must count it via Stats.BlockedTargets instead.
func TestQueryAuthNeverDialsNonPublicServer(t *testing.T) {
	z := map[string][]dns.RR{
		"example.com./A": {mustRR(t, "example.com. 300 IN A 93.184.216.34")},
	}
	rec := &recordingEx{z: z}
	var stats metrics.Stats
	r := NewResolverWithStats(rec, &stats)
	del := Delegation{
		ETLD: "com", NS: []string{"ns1.example.com."},
		NSIPs:         []string{"9.9.9.9", "10.0.0.1"}, // full evidence includes the private address
		DialableNSIPs: []string{"9.9.9.9", "10.0.0.1"}, // simulates a caller that skipped the scope filter
	}
	r.Resolve(context.Background(), "example.com", "sc", "run", del, records.DefaultConfig(), time.Unix(0, 0).UTC(), nil)

	rec.mu.Lock()
	dialed := append([]string(nil), rec.dialed...)
	rec.mu.Unlock()
	for _, d := range dialed {
		if d == "10.0.0.1" {
			t.Fatalf("dialed non-public server 10.0.0.1 directly; dialed=%v", dialed)
		}
	}
	if stats.BlockedTargets.Load() == 0 {
		t.Errorf("expected Stats.BlockedTargets to be incremented for the blocked private target")
	}
}
