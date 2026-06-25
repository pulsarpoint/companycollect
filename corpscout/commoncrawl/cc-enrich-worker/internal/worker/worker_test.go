package worker

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/model"
	"cc-enrich-worker/internal/output"
	"cc-enrich-worker/internal/vec"
)

type multiGetter map[string][]byte

func (m multiGetter) GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	return m[fmt.Sprintf("%s:%d", key, start)], nil
}

type fakeEmbedder struct{ vec []float32 }

func (f fakeEmbedder) Embed(texts []string, instr string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = f.vec
	}
	return out, nil
}

// gzWarc wraps an HTTP response string as a gzipped single WARC response record.
func gzWarc(httpResp string) []byte {
	rec := "WARC/1.0\r\nWARC-Type: response\r\n\r\n" + httpResp
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write([]byte(rec))
	gw.Close()
	return buf.Bytes()
}

func hasTechRow(rows []output.TechRow, name string) bool {
	for _, r := range rows {
		if r.Technology == name {
			return true
		}
	}
	return false
}

func TestProcessShardAndParquet(t *testing.T) {
	page1 := gzWarc("HTTP/1.1 200 OK\r\nServer: nginx\r\n\r\n<html><body>Acme software company</body></html>")
	page2 := gzWarc("HTTP/1.1 200 OK\r\n\r\n<html><head><meta name=\"generator\" content=\"WordPress 6.4\">" +
		"<link href=\"/wp-content/themes/x/style.css\" rel=\"stylesheet\"></head><body>about us</body></html>")
	getter := multiGetter{"f.warc.gz:0": page1, "f.warc.gz:1000": page2}
	items := []model.WorklistItem{
		{RootDomain: "acme.com", URL: "https://acme.com/", WarcFilename: "f.warc.gz", Offset: 0, Length: int64(len(page1)), Primary: true},
		{RootDomain: "acme.com", URL: "https://acme.com/about", WarcFilename: "f.warc.gz", Offset: 1000, Length: int64(len(page2)), Primary: false},
	}
	ref := &model.Reference{Codes: []string{"62.01"}, Labels: []string{"Programming"}, Divisions: []string{"62"},
		M: [][]float32{vec.Norm([]float32{1, 0, 0})}}
	protos := &model.Prototypes{}
	emb := fakeEmbedder{vec: vec.Norm([]float32{1, 0, 0})}
	cfg := ShardConfig{CrawlID: "CC-MAIN-2026-25", SourceRunID: "run1",
		ResolvedAt: time.Unix(1700000000, 0).UTC(), Concurrency: 2}

	res, err := ProcessShard(context.Background(), items, getter, emb, ref, protos, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Domains) != 1 {
		t.Fatalf("want 1 domain, got %d", len(res.Domains))
	}
	d := res.Domains[0]
	if d.NaceCode != "62.01" || d.NaceConfident != 1 {
		t.Fatalf("classify wrong: %+v", d)
	}
	if !hasTechRow(res.Tech, "Nginx") || !hasTechRow(res.Tech, "WordPress") {
		t.Fatalf("tech union wrong: %+v", res.Tech)
	}

	// Parquet round-trip with the migration column order.
	dir := t.TempDir()
	dp, tp := filepath.Join(dir, "domains.parquet"), filepath.Join(dir, "tech.parquet")
	if err := output.WriteDomains(dp, res.Domains); err != nil {
		t.Fatal(err)
	}
	if err := output.WriteTech(tp, res.Tech); err != nil {
		t.Fatal(err)
	}
	back, err := parquet.ReadFile[output.DomainRow](dp)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 1 || back[0].NaceCode != "62.01" || back[0].RootDomain != "acme.com" {
		t.Fatalf("round-trip mismatch: %+v", back)
	}
}

func TestProcessShardIndustryMode(t *testing.T) {
	page1 := gzWarc("HTTP/1.1 200 OK\r\nServer: nginx\r\n\r\n<html><body>Acme software company</body></html>")
	getter := multiGetter{"f.warc.gz:0": page1}
	items := []model.WorklistItem{
		{RootDomain: "acme.com", URL: "https://acme.com/", WarcFilename: "f.warc.gz", Offset: 0, Length: int64(len(page1)), Primary: true},
	}
	ref := &model.Reference{Codes: []string{"62.01"}, Labels: []string{"Programming"}, Divisions: []string{"62"},
		M: [][]float32{vec.Norm([]float32{1, 0, 0})}}
	emb := fakeEmbedder{vec: vec.Norm([]float32{1, 0, 0})}
	cfg := ShardConfig{CrawlID: "C", ResolvedAt: time.Unix(1700000000, 0).UTC(), Concurrency: 1, Mode: "industry"}

	res, err := ProcessShard(context.Background(), items, getter, emb, ref, &model.Prototypes{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Domains) != 1 || res.Domains[0].NaceCode != "62.01" {
		t.Fatalf("industry classify wrong: %+v", res.Domains)
	}
	if len(res.Tech) != 0 {
		t.Fatalf("industry mode should emit no tech rows, got %d", len(res.Tech))
	}
}

func TestProcessShardTechMode(t *testing.T) {
	// mode=res.Tech needs neither embedder nor reference: pass nil and assert no panic/calls.
	page := gzWarc("HTTP/1.1 200 OK\r\nServer: nginx\r\n\r\n<html><head>" +
		"<meta name=\"generator\" content=\"WordPress 6.4\">" +
		"<link href=\"/wp-content/themes/x/style.css\" rel=\"stylesheet\"></head>" +
		"<body>x<footer>LEI: HWUPKR0MPOU8FGXBT394</footer></body></html>")
	getter := multiGetter{"f.warc.gz:0": page}
	items := []model.WorklistItem{
		{RootDomain: "acme.com", URL: "https://acme.com/", WarcFilename: "f.warc.gz", Offset: 0, Length: int64(len(page)), Primary: true},
	}
	cfg := ShardConfig{CrawlID: "C", ResolvedAt: time.Unix(1700000000, 0).UTC(), Concurrency: 1, Mode: "tech"}

	res, err := ProcessShard(context.Background(), items, getter, nil, nil, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Domains) != 0 {
		t.Fatalf("tech mode should emit no domain rows, got %d", len(res.Domains))
	}
	if !hasTechRow(res.Tech, "Nginx") || !hasTechRow(res.Tech, "WordPress") {
		t.Fatalf("tech mode tech rows wrong: %+v", res.Tech)
	}
	if res.Tech[0].RootDomain != "acme.com" {
		t.Fatalf("tech row not keyed to domain: %+v", res.Tech[0])
	}
	if len(res.Identifiers) != 1 || res.Identifiers[0].IDType != "lei" || res.Identifiers[0].IDValue != "HWUPKR0MPOU8FGXBT394" ||
		res.Identifiers[0].Valid != 1 || res.Identifiers[0].RootDomain != "acme.com" {
		t.Fatalf("identifier row wrong: %+v", res.Identifiers)
	}
}
