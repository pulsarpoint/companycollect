package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

const knowledgeDir = "../../internal/detect/knowledge/testdata/providers"

type outLine struct {
	KnowledgeVersion string `json:"knowledge_version"`
	RecordID         string `json:"record_id"`
	Results          []struct {
		ServiceType string `json:"service_type"`
		ProviderKey string `json:"provider_key"`
		ValidFrom   string `json:"valid_from"`
	} `json:"results"`
	Findings []struct {
		Code string `json:"code"`
	} `json:"findings"`
}

func TestResolveStreamsOneLinePerRecord(t *testing.T) {
	stdin := strings.NewReader(`{"record_id":"a","root_domain":"a.se","name":"a.se.","type":"NS","value":"ns1.loopia.se.","first_seen":"2026-09-01","last_seen":"2026-09-23"}
{"record_id":"b","root_domain":"b.se","name":"b.se.","type":"MX","value":"0 ."}
{"record_id":"c","root_domain":"c.se","name":"c.se.","type":"A","value":"192.0.2.1"}`)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"resolve", "-knowledge", knowledgeDir}, stdin, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 lines, got %q", stdout.String())
	}
	var got []outLine
	for _, l := range lines {
		var o outLine
		if err := json.Unmarshal([]byte(l), &o); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(o.KnowledgeVersion, "sha256:") {
			t.Fatalf("knowledge version = %q", o.KnowledgeVersion)
		}
		got = append(got, o)
	}
	if got[0].RecordID != "a" || len(got[0].Results) != 1 || got[0].Results[0].ProviderKey != "loopia" || got[0].Results[0].ValidFrom != "2026-09-01" {
		t.Fatalf("a = %+v", got[0])
	}
	if got[1].RecordID != "b" || len(got[1].Results) != 0 || len(got[1].Findings) != 1 || got[1].Findings[0].Code != "null_mx" {
		t.Fatalf("b = %+v", got[1])
	}
	if got[2].RecordID != "c" || len(got[2].Results)+len(got[2].Findings) != 0 {
		t.Fatalf("c = %+v", got[2])
	}
}

func TestResolveRejectsBadArguments(t *testing.T) {
	for name, args := range map[string][]string{
		"no subcommand":  {},
		"no knowledge":   {"resolve"},
		"missing folder": {"resolve", "-knowledge", t.TempDir()},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(args, strings.NewReader(""), &stdout, &stderr); code == 0 {
			t.Errorf("%s: exit 0", name)
		}
	}
}

func TestResolveRejectsBadRecords(t *testing.T) {
	for name, stdin := range map[string]string{
		"not json":       `{"record_id":`,
		"missing fields": `{"record_id":"x","value":"ns1.loopia.se."}`,
	} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{"resolve", "-knowledge", knowledgeDir}, strings.NewReader(stdin), &stdout, &stderr); code != 1 {
			t.Errorf("%s: exit %d, want 1 (%s)", name, code, stderr.String())
		}
	}
}
