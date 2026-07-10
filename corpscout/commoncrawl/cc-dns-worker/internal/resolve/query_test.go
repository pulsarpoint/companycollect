package resolve

import (
	"context"
	"testing"
	"time"

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
	del := Delegation{ETLD: "com", NS: []string{"ns1.example.com."}, NSIPs: []string{"9.9.9.9"}}

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
	if !find("MX", "", "10 mail.example.com") {
		t.Errorf("MX value should be full rdata '10 mail.example.com'; records=%+v", res.Records)
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
	del := Delegation{ETLD: "com", NS: []string{"ns1.example.com."}, NSIPs: []string{"9.9.9.9"}}

	res := r.Resolve(context.Background(), "example.com", "2026-07-05", "run1", del, records.DefaultConfig(), time.Unix(0, 0).UTC(), nil)

	found := false
	for _, rec := range res.Records {
		if rec.RecordType == "CNAME" && rec.Slot == "www" && rec.Value == "foo.cdn.net" {
			found = true
		}
	}
	if !found {
		t.Errorf("missing CNAME record for www (slot=www, value=foo.cdn.net); records=%+v", res.Records)
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
	del := Delegation{ETLD: "com", NS: []string{"ns1.example.com."}, NSIPs: []string{"9.9.9.9"}}
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
