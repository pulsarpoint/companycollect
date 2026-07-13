package worker

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/model"
	"cc-enrich-worker/internal/output"
	"cc-enrich-worker/internal/tech"
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
	if len(res.Domains) != 1 || res.Domains[0].RootDomain != "acme.com" {
		t.Fatalf("want 1 domain master row for acme.com, got %+v", res.Domains)
	}
	if len(res.Industries) < 1 || res.Industries[0].NaceCode != "62.01" || res.Industries[0].Rank != 1 || res.Industries[0].IsPrimary != 1 {
		t.Fatalf("industry classify wrong: %+v", res.Industries)
	}
	if len(res.PageSignals) != 1 {
		t.Fatalf("want 1 page-signal row, got %d", len(res.PageSignals))
	}
	if !hasTechRow(res.Tech, "Nginx") || !hasTechRow(res.Tech, "WordPress") {
		t.Fatalf("tech union wrong: %+v", res.Tech)
	}

	// Parquet round-trip on the new industry rows (the classification output).
	ip := filepath.Join(t.TempDir(), "industries.parquet")
	if err := output.WriteIndustries(ip, res.Industries); err != nil {
		t.Fatal(err)
	}
	back, err := parquet.ReadFile[output.IndustryRow](ip)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) < 1 || back[0].NaceCode != "62.01" || back[0].RootDomain != "acme.com" {
		t.Fatalf("round-trip mismatch: %+v", back)
	}
}

// "both" pays the GPU for every primary page's vector; the rows must reach ShardResult.Embeddings
// (the streamer's embeddings.parquet) instead of being dropped after classification.
func TestProcessShardBothModeEmitsEmbeddings(t *testing.T) {
	page1 := gzWarc("HTTP/1.1 200 OK\r\nServer: nginx\r\n\r\n<html><body>Acme software company</body></html>")
	page2 := gzWarc("HTTP/1.1 200 OK\r\n\r\n<html><body>about us</body></html>")
	getter := multiGetter{"f.warc.gz:0": page1, "f.warc.gz:1000": page2}
	items := []model.WorklistItem{
		{RootDomain: "acme.com", URL: "https://acme.com/", WarcFilename: "f.warc.gz", Offset: 0, Length: int64(len(page1)), Primary: true},
		{RootDomain: "acme.com", URL: "https://acme.com/about", WarcFilename: "f.warc.gz", Offset: 1000, Length: int64(len(page2)), Primary: false},
	}
	ref := &model.Reference{Codes: []string{"62.01"}, Labels: []string{"Programming"}, Divisions: []string{"62"},
		M: [][]float32{vec.Norm([]float32{1, 0, 0})}}
	emb := fakeEmbedder{vec: vec.Norm([]float32{1, 0, 0})}
	cfg := ShardConfig{CrawlID: "C", SourceRunID: "run1", ResolvedAt: time.Unix(1700000000, 0).UTC(),
		Concurrency: 2, Mode: "both"}

	res, err := ProcessShard(context.Background(), items, getter, emb, ref, &model.Prototypes{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Embeddings) != 1 {
		t.Fatalf("both mode: want 1 embedding row for the primary page, got %d (%+v)", len(res.Embeddings), res.Embeddings)
	}
	e := res.Embeddings[0]
	if e.RootDomain != "acme.com" || e.SourceURL != "https://acme.com/" ||
		e.WarcFilename != "f.warc.gz" || e.WarcOffset != 0 || e.WarcLength != int64(len(page1)) {
		t.Fatalf("embedding row provenance must point at the primary page: %+v", e)
	}
	if e.EmbedDim != 3 || len(e.Embedding) != 3 || e.TextLen == 0 {
		t.Fatalf("embedding row vector wrong: dim=%d len=%d textlen=%d", e.EmbedDim, len(e.Embedding), e.TextLen)
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
	if len(res.Domains) != 1 {
		t.Fatalf("want 1 domain master row, got %d", len(res.Domains))
	}
	if len(res.Industries) < 1 || res.Industries[0].NaceCode != "62.01" {
		t.Fatalf("industry classify wrong: %+v", res.Industries)
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
	if len(res.Domains) != 1 || res.Domains[0].RootDomain != "acme.com" {
		t.Fatalf("tech mode should emit the domain master row, got %+v", res.Domains)
	}
	if len(res.Industries) != 0 || len(res.PageSignals) != 0 {
		t.Fatalf("tech mode should emit no industries/signals, got %d/%d", len(res.Industries), len(res.PageSignals))
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
	if len(res.Security) != 1 || res.Security[0].RootDomain != "acme.com" {
		t.Fatalf("tech mode primary-page security row missing: %+v", res.Security)
	}
	if len(res.PageMeta) != 1 || res.PageMeta[0].RootDomain != "acme.com" {
		t.Fatalf("tech mode primary-page metadata row missing: %+v", res.PageMeta)
	}
}

// The industry stream must expose its fetch counts so the driver can refuse to commit a
// systemically-failed part (same contract the chunked path enforces via FetchedChunk.Pages/Errs).
func TestProcessIndustryStreamReportsFetchCounts(t *testing.T) {
	page := gzWarc("HTTP/1.1 200 OK\r\n\r\n<html><body>Acme software company</body></html>")
	getter := multiGetter{"ok.warc.gz:0": page} // a.com fetches; b.com and c.com fail
	items := []model.WorklistItem{
		{RootDomain: "a.com", URL: "https://a.com/", WarcFilename: "ok.warc.gz", Offset: 0, Length: int64(len(page)), Primary: true},
		{RootDomain: "b.com", URL: "https://b.com/", WarcFilename: "missing.warc.gz", Offset: 0, Length: 100, Primary: true},
		{RootDomain: "c.com", URL: "https://c.com/", WarcFilename: "missing.warc.gz", Offset: 200, Length: 100, Primary: true},
	}
	ref := &model.Reference{Codes: []string{"62.01"}, Labels: []string{"Programming"}, Divisions: []string{"62"},
		M: [][]float32{vec.Norm([]float32{1, 0, 0})}}
	emb := fakeEmbedder{vec: vec.Norm([]float32{1, 0, 0})}
	cfg := ShardConfig{CrawlID: "C", ResolvedAt: time.Unix(1700000000, 0).UTC(), Concurrency: 2, Mode: "industry"}

	res, err := ProcessIndustryStream(context.Background(), items, getter, emb, ref, &model.Prototypes{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Pages != 3 || res.Errs != 2 {
		t.Fatalf("fetch counts: pages=%d errs=%d, want 3/2", res.Pages, res.Errs)
	}
	if len(res.Domains) != 1 || res.Domains[0].RootDomain != "a.com" {
		t.Fatalf("want the one surviving domain, got %+v", res.Domains)
	}
	if len(res.Embeddings) != 1 || res.Embeddings[0].RootDomain != "a.com" {
		t.Fatalf("want the surviving domain's embedding, got %+v", res.Embeddings)
	}
}

func TestFinalizePreservesTechnologyConfidence(t *testing.T) {
	fetched := FetchedChunk{pages: []pageResult{{
		source: model.WorklistItem{
			RootDomain: "example.com", URL: "https://example.com/", WarcIndex: 7,
			WarcFilename: "example.warc.gz", Offset: 10, Length: 20,
		},
		tech: []model.Technology{{Name: "Odoo", Category: "CMS", Confidence: 25}},
	}}}

	result, err := Finalize(context.Background(), fetched, nil, nil, nil, ShardConfig{Mode: "tech"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Tech) != 1 || result.Tech[0].Confidence != 25 {
		t.Fatalf("technology confidence was not preserved: %+v", result.Tech)
	}
}

func TestProcessPageFullScanDetectsLateTechnology(t *testing.T) {
	matcher, err := tech.NewFastMatcher()
	if err != nil {
		t.Fatal(err)
	}
	tech.SetFastMatcher(matcher)
	t.Cleanup(func() { tech.SetFastMatcher(nil) })

	body := "<html><body>" + strings.Repeat("x", 160<<10) +
		`<meta name="generator" content="WordPress 6.4"></body></html>`
	raw := gzWarc("HTTP/1.1 200 OK\r\n\r\n" + body)
	getter := multiGetter{"late.warc.gz:0": raw}
	item := model.WorklistItem{
		RootDomain: "example.com", URL: "https://example.com/", WarcFilename: "late.warc.gz",
		Offset: 0, Length: int64(len(raw)),
	}

	full, err := processPage(context.Background(), getter, ShardConfig{Mode: "tech"}, item, &chunkStats{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasTech(full.tech, "WordPress") {
		t.Fatalf("full-page scan missed technology after 160 KiB: %+v", full.tech)
	}
}

func TestTechAndMetadataRowsRetainPageProvenance(t *testing.T) {
	home := gzWarc("HTTP/1.1 200 OK\r\nServer: nginx\r\n\r\n<html><head>" +
		`<script type="application/ld+json">{"@type":"Organization","name":"Acme Home"}</script>` +
		"</head><body>home</body></html>")
	about := gzWarc("HTTP/1.1 200 OK\r\n\r\n<html><head>" +
		`<meta name="generator" content="WordPress 6.4">` +
		`<link href="/wp-content/themes/x/style.css" rel="stylesheet">` +
		`<script type="application/ld+json">{"@type":"Organization","name":"Acme Shop"}</script>` +
		"</head><body>about</body></html>")
	getter := multiGetter{
		"first.warc.gz:1234":  home,
		"second.warc.gz:5678": about,
	}
	items := []model.WorklistItem{
		{RootDomain: "acme.com", URL: "https://acme.com/", WarcIndex: 11, WarcFilename: "first.warc.gz", Offset: 1234, Length: int64(len(home)), Primary: true},
		{RootDomain: "acme.com", URL: "https://shop.acme.com/about", WarcIndex: 29, WarcFilename: "second.warc.gz", Offset: 5678, Length: int64(len(about))},
	}
	resolvedAt := time.Unix(1700000000, 0).UTC()
	res, err := ProcessShard(context.Background(), items, getter, nil, nil, nil, ShardConfig{
		CrawlID: "CC-MAIN-2026-25", SourceRunID: "run-pages", ResolvedAt: resolvedAt,
		Concurrency: 2, Mode: "tech",
	})
	if err != nil {
		t.Fatal(err)
	}

	techByName := make(map[string]output.TechRow)
	for _, row := range res.Tech {
		techByName[row.Technology] = row
	}
	nginx := techByName["Nginx"]
	assertTechProvenance(t, nginx, "https://acme.com/", "", 11, "first.warc.gz", 1234, uint64(len(home)))
	wordPress := techByName["WordPress"]
	assertTechProvenance(t, wordPress, "https://shop.acme.com/about", "shop", 29, "second.warc.gz", 5678, uint64(len(about)))

	if len(res.Metadata) != 2 {
		t.Fatalf("want one metadata row per page, got %+v", res.Metadata)
	}
	metadataByURL := make(map[string]output.MetadataRow)
	for _, row := range res.Metadata {
		metadataByURL[row.PageURL] = row
	}
	homeMetadata := metadataByURL["https://acme.com/"]
	if homeMetadata.Name != "Acme Home" {
		t.Fatalf("home metadata lost: %+v", homeMetadata)
	}
	assertMetadataProvenance(t, homeMetadata, "https://acme.com/", "", 11, "first.warc.gz", 1234, uint64(len(home)))
	shopMetadata := metadataByURL["https://shop.acme.com/about"]
	if shopMetadata.Name != "Acme Shop" {
		t.Fatalf("shop metadata lost: %+v", shopMetadata)
	}
	assertMetadataProvenance(t, shopMetadata, "https://shop.acme.com/about", "shop", 29, "second.warc.gz", 5678, uint64(len(about)))
}

func assertTechProvenance(t *testing.T, row output.TechRow, pageURL, subdomain string, warcIndex uint32, warcFilename string, offset, length uint64) {
	t.Helper()
	if row.PageURL != pageURL || row.RootDomain != "acme.com" || row.Subdomain != subdomain ||
		row.WarcIndex != warcIndex || row.WarcFilename != warcFilename ||
		row.WarcRecordOffset != offset || row.WarcRecordLength != length {
		t.Fatalf("technology page provenance mismatch: %+v", row)
	}
}

func assertMetadataProvenance(t *testing.T, row output.MetadataRow, pageURL, subdomain string, warcIndex uint32, warcFilename string, offset, length uint64) {
	t.Helper()
	if row.PageURL != pageURL || row.RootDomain != "acme.com" || row.Subdomain != subdomain ||
		row.WarcIndex != warcIndex || row.WarcFilename != warcFilename ||
		row.WarcRecordOffset != offset || row.WarcRecordLength != length {
		t.Fatalf("metadata page provenance mismatch: %+v", row)
	}
}

func TestProcessShardDecodesLatin1(t *testing.T) {
	latin1 := "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=iso-8859-1\">" +
		"<script type=\"application/ld+json\">{\"@type\":\"Organization\",\"name\":\"M\xfcller GmbH\"}</script>" +
		"</head><body>M\xfcller GmbH</body></html>"
	page := gzWarc("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=iso-8859-1\r\n\r\n" + latin1)
	getter := multiGetter{"f.warc.gz:0": page}
	items := []model.WorklistItem{
		{RootDomain: "mueller.de", URL: "https://mueller.de/", WarcFilename: "f.warc.gz", Offset: 0, Length: int64(len(page)), Primary: true},
	}
	cfg := ShardConfig{CrawlID: "C", ResolvedAt: time.Unix(1700000000, 0).UTC(), Concurrency: 1, Mode: "tech"}
	res, err := ProcessShard(context.Background(), items, getter, nil, nil, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Metadata) != 1 || res.Metadata[0].Name != "Müller GmbH" {
		t.Fatalf("latin-1 JSON-LD name mangled: %+v", res.Metadata)
	}
	if len(res.PageMeta) != 1 || res.PageMeta[0].Charset == "" {
		t.Fatalf("detected charset not backfilled: %+v", res.PageMeta)
	}
}

func TestProcessShardMicrodataProfileFallback(t *testing.T) {
	page := gzWarc("HTTP/1.1 200 OK\r\n\r\n<html><body>" +
		`<div itemscope itemtype="http://schema.org/Dentist"><span itemprop="name">Smile Dental</span>` +
		`<a itemprop="email" href="mailto:info@smile.de">m</a>` +
		`<span itemprop="telephone">+49 30 1234567</span></div>` +
		"</body></html>")
	getter := multiGetter{"f.warc.gz:0": page}
	items := []model.WorklistItem{
		{RootDomain: "smile.de", URL: "https://smile.de/", WarcFilename: "f.warc.gz", Offset: 0, Length: int64(len(page)), Primary: true},
	}
	cfg := ShardConfig{CrawlID: "C", ResolvedAt: time.Unix(1700000000, 0).UTC(), Concurrency: 1, Mode: "tech"}
	res, err := ProcessShard(context.Background(), items, getter, nil, nil, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Metadata) != 1 || res.Metadata[0].Name != "Smile Dental" || res.Metadata[0].Source != "microdata" {
		t.Fatalf("microdata profile not used: %+v", res.Metadata)
	}
	// Profile-derived contact rows must carry the profile's provenance, not a hardcoded "jsonld".
	emailRows, phoneRows := 0, 0
	for _, c := range res.Contacts {
		switch {
		case c.ContactType == "email" && c.Value == "info@smile.de" && c.Source == "microdata":
			emailRows++
		case c.ContactType == "phone" && c.Source == "microdata":
			phoneRows++
		}
	}
	if emailRows != 1 || phoneRows != 1 {
		t.Fatalf("microdata contact provenance wrong (email=%d phone=%d): %+v", emailRows, phoneRows, res.Contacts)
	}
}

// A page can carry a partial JSON-LD org (profile wins, Source "jsonld") AND microdata
// identifiers the JSON-LD lacks — the identifiers must be unioned, never dropped.
func TestProcessShardUnionsMicrodataIdentifiersWithPartialJSONLD(t *testing.T) {
	page := gzWarc("HTTP/1.1 200 OK\r\n\r\n<html><head>" +
		`<script type="application/ld+json">{"@type":"Organization","name":"Acme"}</script>` +
		"</head><body>" +
		`<div itemscope itemtype="http://schema.org/Organization"><span itemprop="vatID">DE136695976</span></div>` +
		"</body></html>")
	getter := multiGetter{"f.warc.gz:0": page}
	items := []model.WorklistItem{
		{RootDomain: "acme.de", URL: "https://acme.de/", WarcFilename: "f.warc.gz", Offset: 0, Length: int64(len(page)), Primary: true},
	}
	cfg := ShardConfig{CrawlID: "C", ResolvedAt: time.Unix(1700000000, 0).UTC(), Concurrency: 1, Mode: "tech"}
	res, err := ProcessShard(context.Background(), items, getter, nil, nil, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Metadata) != 1 || res.Metadata[0].Name != "Acme" || res.Metadata[0].Source != "jsonld" {
		t.Fatalf("JSON-LD profile should win: %+v", res.Metadata)
	}
	vats := 0
	for _, id := range res.Identifiers {
		if id.IDType == "vat" && id.IDValue == "DE136695976" && id.Source == "microdata" {
			vats++
		}
	}
	if vats != 1 {
		t.Fatalf("microdata VAT identifier dropped alongside partial JSON-LD: %+v", res.Identifiers)
	}
}
