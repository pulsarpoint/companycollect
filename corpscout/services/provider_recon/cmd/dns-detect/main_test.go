package main

import (
	"bytes"
	"context"
	"os"
	"time"

	"encoding/json"
	"provider_recon/internal/publish"
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

func TestResolveStopsAtAnIncompleteRecordAfterWritingEarlierLines(t *testing.T) {
	stdin := strings.NewReader(`{"record_id":"a","root_domain":"a.se","name":"a.se.","type":"NS","value":"ns1.loopia.se."}
{"record_id":"","root_domain":"b.se","name":"b.se.","type":"NS","value":"ns1.loopia.se."}`)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"resolve", "-knowledge", knowledgeDir}, stdin, &stdout, &stderr); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if lines := strings.Split(strings.TrimSpace(stdout.String()), "\n"); len(lines) != 1 || !strings.Contains(lines[0], `"record_id":"a"`) {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "record 2") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestServeNeedsAStore(t *testing.T) {
	for _, k := range []string{"CORPSCOUT_S3_ENDPOINT", "CORPSCOUT_S3_ACCESS_KEY", "CORPSCOUT_S3_SECRET_KEY"} {
		t.Setenv(k, "")
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"serve"}, strings.NewReader(""), &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "CORPSCOUT_S3") {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
}

func TestServeRefusesToStartWithoutKnowledge(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := serve(context.Background(), []string{"-store", t.TempDir(), "-listen", "127.0.0.1:0"}, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "load knowledge") {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	_ = stdout
}

func TestServeStartsAndStopsCleanly(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store := publish.FSStore{Root: dir}
	doc, err := os.ReadFile(knowledgeDir + "/loopia.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, publish.LatestKey("loopia"), doc, "application/json"); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, publish.IndexKey, []byte(`{"runs":[],"providers":["loopia"]}`), "application/json"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan int, 1)
	var stderr bytes.Buffer
	go func() { done <- serve(ctx, []string{"-store", dir, "-listen", "127.0.0.1:0"}, &stderr) }()
	time.Sleep(300 * time.Millisecond)
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit %d: %s", code, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not stop")
	}
}
