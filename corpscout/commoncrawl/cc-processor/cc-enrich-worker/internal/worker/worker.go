package worker

import (
	"context"
	"log"
	"log/slog"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"

	"cc-enrich-worker/internal/classify"
	"cc-enrich-worker/internal/extract"
	"cc-enrich-worker/internal/model"
	"cc-enrich-worker/internal/output"
	"cc-enrich-worker/internal/parse"
	"cc-enrich-worker/internal/security"
	"cc-enrich-worker/internal/tech"
	"cc-raw/fetch"
)

const (
	ccBucket = "commoncrawl"
)

const classifyInstruction = "Classify the business into its industry category"

type embedderIface = classify.Embedder

// ShardResult bundles the per-pass outputs. Each maps to its own ClickHouse table.
type ShardResult struct {
	Domains     []output.DomainRow // identity master, written by every pass
	Industries  []output.IndustryRow
	PageSignals []output.PageSignalRow
	Tech        []output.TechRow
	Identifiers []output.IdentifierRow
	JSONLD      []output.JSONLDRow    // every typed/identified JSON-LD entity, from the tech pass
	Contacts    []output.ContactRow   // emails/phones/social, from the tech pass
	Security    []output.SecurityRow  // full response-header map, from the tech pass
	PageMeta    []output.PageMetaRow  // head content signals (title/meta/hreflang/jsonld @types), tech pass
	Embeddings  []output.EmbeddingRow // raw page vectors, from the industry pass (saved separately)

	// Pages/Errs carry the industry stream's fetch counts so the driver can enforce the same
	// failure contract as the chunked path (FetchedChunk.Pages/Errs + maxFetchErrorRate): a
	// systemically-failed fetch must not commit a near-empty part that then gets marked done.
	Pages, Errs int64
}

func b2u8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

// divisionOf is the NACE division: the part of a code before the first '.' ("29.31" -> "29").
func divisionOf(code string) string {
	if i := strings.IndexByte(code, '.'); i >= 0 {
		return code[:i]
	}
	return code
}

func domainRow(cfg ShardConfig, root, url, sub string) output.DomainRow {
	return output.DomainRow{
		CrawlID: cfg.CrawlID, URL: url, RootDomain: root, Subdomain: sub,
		SourceURL: url, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
	}
}

// industryRows fans a classified domain's ranked candidates into one row per (domain, nace_code):
// rank 1..N by score, is_primary on the top match. Empty for junk/keyword pages (no candidates).
func industryRows(cfg ShardConfig, root, url string, res model.DomainResult) []output.IndustryRow {
	n := len(res.Top3Codes)
	if len(res.Top3Labels) < n {
		n = len(res.Top3Labels)
	}
	if len(res.Top3Scores) < n {
		n = len(res.Top3Scores)
	}
	rows := make([]output.IndustryRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, output.IndustryRow{
			CrawlID: cfg.CrawlID, RootDomain: root,
			NaceCode: res.Top3Codes[i], NaceLabel: res.Top3Labels[i], NaceDivision: divisionOf(res.Top3Codes[i]),
			Rank: uint8(i + 1), IsPrimary: b2u8(i == 0), Score: res.Top3Scores[i], NaceMethod: res.NaceMethod,
			SourceURL: url, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
		})
	}
	return rows
}

func pageSignalRow(cfg ShardConfig, root, url, sub string, res model.DomainResult) output.PageSignalRow {
	return output.PageSignalRow{
		CrawlID: cfg.CrawlID, RootDomain: root, Subdomain: sub, SourceURL: url,
		PageType: res.PageType, PageTypeScore: res.PageTypeScore,
		NaceConfident: b2u8(res.NaceConfident), NaceMargin: res.NaceMargin,
		SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
	}
}

func contactRow(cfg ShardConfig, root, url, typ, value, source string) output.ContactRow {
	return output.ContactRow{
		CrawlID: cfg.CrawlID, RootDomain: root, ContactType: typ, Value: value, Source: source,
		SourceURL: url, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
	}
}

// embeddingRow keeps the raw page vector (the expensive GPU artifact) so it never has to be recomputed,
// plus the page metadata to identify and re-fetch the exact source page.
func embeddingRow(cfg ShardConfig, b streamItem, vec []float32) output.EmbeddingRow {
	return output.EmbeddingRow{
		CrawlID: cfg.CrawlID, RootDomain: b.root, Subdomain: b.sub,
		Embedding: vec, EmbedDim: uint16(len(vec)), TextLen: uint32(len(b.text)),
		SourceURL: b.url, WarcFilename: b.warc, WarcOffset: b.off, WarcLength: b.length,
		SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
	}
}

type ShardConfig struct {
	CrawlID, SourceRunID string
	ResolvedAt           time.Time
	Concurrency          int
	EmbedConcurrency     int    // industry stream: embed requests kept continuously in flight
	EmbedBatch           int    // industry stream: texts per embed request
	TechMaxBytes         int    // body cap fed to Wappalyzer; 0 => no cap (full body)
	Mode                 string // "industry" | "tech" | "both" | "embed" (default "both")
	EmbedOnly            bool   // embed mode: fetch+embed+keep the vector, skip NACE classify & rows

	// RunStats, when non-nil, is the process-wide sink the range runner uses to aggregate every
	// chunk's fetch/tech/page counters into ONE periodic stats line. Setting it also SUPPRESSES the
	// per-chunk timing/page + "S3 range reads scope=fetch_chunk" logs and the industry stream's own
	// progress ticker — the interleaving noise the aggregate line replaces. nil (a single --part run)
	// keeps the original per-chunk logging byte-for-byte and skips aggregation.
	RunStats *RunStats
}

// RunStats accumulates fetch diagnostics across every chunk of every part in a range run so the
// runner can print one cumulative line. All fields are updated atomically; the ticker only reads via
// Snapshot. A nil *RunStats is never dereferenced — ShardConfig.RunStats == nil is the single-part path.
type RunStats struct {
	pages, errs, fetchNs, parseNs, techNs int64
}

func (r *RunStats) add(pages, errs, fetchNs, parseNs, techNs int64) {
	atomic.AddInt64(&r.pages, pages)
	atomic.AddInt64(&r.errs, errs)
	atomic.AddInt64(&r.fetchNs, fetchNs)
	atomic.AddInt64(&r.parseNs, parseNs)
	atomic.AddInt64(&r.techNs, techNs)
}

// RunStatsSnapshot is a consistent-enough atomic read of RunStats for the formatter (each field is
// loaded atomically; the set is not a single transaction, which is fine for a progress line).
type RunStatsSnapshot struct {
	Pages, Errs, FetchNs, ParseNs, TechNs int64
}

func (r *RunStats) Snapshot() RunStatsSnapshot {
	return RunStatsSnapshot{
		Pages:   atomic.LoadInt64(&r.pages),
		Errs:    atomic.LoadInt64(&r.errs),
		FetchNs: atomic.LoadInt64(&r.fetchNs),
		ParseNs: atomic.LoadInt64(&r.parseNs),
		TechNs:  atomic.LoadInt64(&r.techNs),
	}
}

type domainAgg struct {
	rootDomain, primaryURL, primarySub, primaryText string
	primaryWarc                                     string // primary page's WARC record coords — re-fetch
	primaryOff, primaryLen                          int64  // provenance carried onto the embedding row
	identifiers                                     []model.Identifier
	security                                        map[string]string // primary page's response headers
	meta                                            parse.HeadMeta    // primary page's head signals
	jsonldTypes                                     []string          // primary page's JSON-LD @types
	hasPrimary                                      bool
}

// FetchedChunk holds the per-domain aggregates produced by FetchChunk, ready for Finalize.
// (domainAgg is unexported, so callers pass this opaque value between the two phases.) Pages/Errs carry
// the chunk's fetch counts so the caller can enforce a per-shard failure contract (a systemically-failed
// fetch must not write a near-empty part that then gets loaded + marked done).
type FetchedChunk struct {
	aggs        []*domainAgg
	pages       []pageResult
	Pages, Errs int64
}

// subdomainOf returns the label(s) before the registered root, or "" for apex/www.
func subdomainOf(rawURL, root string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	if host == root || host == "" {
		return ""
	}
	if strings.HasSuffix(host, "."+root) {
		return strings.TrimSuffix(host, "."+root)
	}
	return ""
}

// contentTypeOf returns the response Content-Type header (any casing), or "".
func contentTypeOf(headers map[string][]string) string {
	for k, v := range headers {
		if strings.EqualFold(k, "Content-Type") && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

func modeFlags(cfg ShardConfig) (mode string, runEmbed, runTech bool) {
	mode = cfg.Mode
	if mode == "" {
		mode = "both"
	}
	return mode, mode != "tech", mode != "industry"
}

// PageConcurrency is the per-domain page pool: how many of ONE domain's pages fetch in parallel
// inside processDomain. Total in-flight fetches for a tech/both chunk are therefore capped at
// Concurrency × PageConcurrency (Concurrency = the DOMAIN pool) — size the S3 transport to that
// product. All fetches hit the single CommonCrawl S3 endpoint (never the companies' own sites),
// so there is no per-domain politeness concern.
const PageConcurrency = 8

// chunkStats accumulates per-chunk fetch diagnostics across all domain/page goroutines.
type chunkStats struct {
	fetchNs, parseNs, techNs, pages, errs int64 // updated atomically
	errOnce                               sync.Once
	errSample                             string
}

func s3Stats(getter fetch.RangeGetter) fetch.S3Stats {
	if getter, ok := getter.(*fetch.S3Getter); ok {
		return getter.Stats()
	}
	return fetch.S3Stats{}
}

func logS3Stats(ctx context.Context, getter fetch.RangeGetter, previous fetch.S3Stats, elapsed time.Duration, scope string) {
	s3Getter, ok := getter.(*fetch.S3Getter)
	if !ok {
		return
	}
	stats := s3Getter.Stats().Delta(previous)
	if stats.GetObjectCalls == 0 {
		return
	}
	getObjectAvg := stats.GetObjectTime / time.Duration(stats.GetObjectCalls)
	headerAvg := time.Duration(0)
	if stats.HTTPAttempts > 0 {
		headerAvg = stats.HTTPHeaderTime / time.Duration(stats.HTTPAttempts)
	}
	bodyAvg := time.Duration(0)
	if stats.BodyReadAttempts > 0 {
		bodyAvg = stats.BodyReadTime / time.Duration(stats.BodyReadAttempts)
	}
	throughputMiB := 0.0
	requestsPerSecond := 0.0
	if elapsed > 0 {
		throughputMiB = float64(stats.BodyBytes) / (1024 * 1024) / elapsed.Seconds()
		requestsPerSecond = float64(stats.HTTPAttempts) / elapsed.Seconds()
	}
	sdkRetryAttempts := stats.HTTPAttempts - stats.HeadObjectCalls - stats.GetObjectCalls
	if sdkRetryAttempts < 0 {
		sdkRetryAttempts = 0
	}
	slog.InfoContext(ctx, "S3 range reads",
		"scope", scope,
		"head_object_calls", stats.HeadObjectCalls,
		"get_object_calls", stats.GetObjectCalls,
		"http_attempts", stats.HTTPAttempts,
		"requests_per_second", requestsPerSecond,
		"sdk_retry_attempts", sdkRetryAttempts,
		"http_429", stats.HTTP429s,
		"http_503", stats.HTTP503s,
		"get_object_avg_ms", float64(getObjectAvg.Microseconds())/1000,
		"http_header_avg_ms", float64(headerAvg.Microseconds())/1000,
		"body_read_avg_ms", float64(bodyAvg.Microseconds())/1000,
		"body_read_errors", stats.BodyReadErrors,
		"body_read_retries", stats.BodyReadRetries,
		"body_mib", float64(stats.BodyBytes)/(1024*1024),
		"wall_throughput_mib_s", throughputMiB,
	)
}

func (s *chunkStats) recordErr(err error) {
	atomic.AddInt64(&s.errs, 1)
	s.errOnce.Do(func() { s.errSample = err.Error() })
}

// pageResult is everything ONE page contributed — produced by processPage with no shared state,
// merged deterministically by mergePageResults. primary marks the result carrying the embed text
// (the worklist's Primary page, embed/both modes only).
type pageResult struct {
	source      model.WorklistItem
	sub         string
	primary     bool
	text        string // parsed visible text (embed/both, Primary page only)
	tech        []model.Technology
	emails      []string
	ids         []model.Identifier
	jsonld      []model.JSONLDEntity
	microdata   model.CompanyProfile
	security    map[string]string // response headers, Primary page only
	meta        parse.HeadMeta    // head signals, Primary page only
	jsonldTypes []string          // JSON-LD @types, Primary page only
}

// processPage fetches ONE page and runs the per-page extraction for the configured mode:
// tech detection (body capped at TechMaxBytes) + emails + JSON-LD entities + LEI/VAT (tech/both),
// and the visible-text parse of the Primary page (embed/both). Pure with respect to the chunk —
// it returns a pageResult and touches no aggregate.
func processPage(ctx context.Context, getter fetch.RangeGetter, cfg ShardConfig, it model.WorklistItem, stats *chunkStats) (pageResult, error) {
	_, runEmbed, runTech := modeFlags(cfg)
	if it.Offset < 0 || it.Length <= 0 {
		err := errors.Newf("invalid WARC record coordinates: offset=%d length=%d", it.Offset, it.Length)
		atomic.AddInt64(&stats.pages, 1)
		stats.recordErr(err)
		return pageResult{}, err
	}

	t0 := time.Now()
	fctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	headers, body, err := fetch.FetchRecord(fctx, getter, ccBucket, it.WarcFilename, it.Offset, it.Length)
	cancel()
	atomic.AddInt64(&stats.fetchNs, time.Since(t0).Nanoseconds())
	atomic.AddInt64(&stats.pages, 1)
	if err != nil {
		stats.recordErr(err)
		return pageResult{}, err // a bad/slow page is skipped; the domain still emits if any page survived
	}

	decoded, encName := parse.DecodeHTML(body, contentTypeOf(headers))
	r := pageResult{source: it, sub: subdomainOf(it.URL, it.RootDomain), primary: it.Primary}
	if runTech {
		// Cap the body ONLY for the expensive Wappalyzer regex scan (~1.2s/page uncapped). The cheap
		// linear extractors run on the FULL body — LEI/VAT/JSON-LD live in page footers, past the cap.
		techBody := body
		if limit := cfg.TechMaxBytes; limit > 0 && len(techBody) > limit {
			techBody = techBody[:limit]
		}
		t2 := time.Now()
		r.tech = tech.DetectTech(headers, techBody)
		atomic.AddInt64(&stats.techNs, time.Since(t2).Nanoseconds())
		decodedStr := string(decoded) // one shared copy: Emails + ParseHeadMeta both take strings
		r.emails = parse.Emails(decodedStr)
		r.jsonld, r.ids = extract.ExtractJSONLD(decoded)
		// Microdata is retained for contacts and identifiers. It is not written to the JSON-LD
		// evidence table and never competes with or replaces any JSON-LD entity.
		if mdProf, mdIDs := extract.ExtractProfileMicrodata(decoded, it.URL); !mdProf.Empty() || len(mdIDs) > 0 {
			r.microdata = mdProf
			r.ids = append(r.ids, mdIDs...)
		}
		r.ids = append(r.ids, extract.ExtractLEIs(decoded)...)
		r.ids = append(r.ids, extract.ExtractVATs(decoded)...)
		r.ids = append(r.ids, extract.Trackers(decoded)...)
		if it.Primary {
			r.security = security.HeaderMap(headers)
			r.meta = parse.ParseHeadMeta(decodedStr)
			if r.meta.Charset == "" {
				r.meta.Charset = encName // head declared nothing — record what we detected
			}
			r.jsonldTypes = extract.JSONLDTypeNames(r.jsonld)
		}
	}
	if runEmbed && it.Primary {
		t1 := time.Now()
		r.text = parse.MainText(decoded, it.URL)
		atomic.AddInt64(&stats.parseNs, time.Since(t1).Nanoseconds())
	}
	return r, nil
}

// mergePageResults folds one domain's per-page results into its aggregate, in WORKLIST order
// (slice index = the page's rank) — deterministic regardless of the parallel completion order:
// the primary row = the lowest-rank survivor and identifier dedup is first-rank-wins.
// ok[i] marks pages that fetched successfully; a domain with no survivor yields an empty agg
// (no primaryURL), which Finalize drops.
func mergePageResults(dom string, results []pageResult, ok []bool) *domainAgg {
	agg := &domainAgg{rootDomain: dom}
	idSeen := map[string]bool{}
	for i, r := range results {
		if !ok[i] {
			continue
		}
		if agg.primaryURL == "" { // lowest-rank surviving page keys this domain's rows
			agg.primaryURL = r.source.URL
			agg.primarySub = r.sub
		}
		for _, id := range r.ids {
			if !idSeen[id.Value] {
				idSeen[id.Value] = true
				agg.identifiers = append(agg.identifiers, id)
			}
		}
		if r.primary && !agg.hasPrimary {
			agg.hasPrimary = true
			agg.primaryText = r.text
			agg.primaryWarc = r.source.WarcFilename
			agg.primaryOff = r.source.Offset
			agg.primaryLen = r.source.Length
			agg.security = r.security
			agg.meta = r.meta
			agg.jsonldTypes = r.jsonldTypes
		}
	}
	return agg
}

// processDomain is ONE unit of work: fetch a domain's pages through the domain's own bounded page
// pool (PageConcurrency) and merge them into the domain aggregate. A failed page is recorded in
// stats and skipped — it never fails the domain.
func processDomain(ctx context.Context, getter fetch.RangeGetter, cfg ShardConfig, dom string, pages []model.WorklistItem, stats *chunkStats) (*domainAgg, []pageResult) {
	results := make([]pageResult, len(pages))
	ok := make([]bool, len(pages))
	var g errgroup.Group
	g.SetLimit(PageConcurrency)
	for i, it := range pages {
		g.Go(func() error {
			if r, err := processPage(ctx, getter, cfg, it, stats); err == nil {
				results[i], ok[i] = r, true
			}
			return nil // page errors are recorded in stats, never propagated
		})
	}
	_ = g.Wait()
	successful := make([]pageResult, 0, len(results))
	for i, result := range results {
		if ok[i] {
			successful = append(successful, result)
		}
	}
	return mergePageResults(dom, results, ok), successful
}

// FetchChunk downloads + parses every page of the chunk into per-domain aggregates; tech detection
// and structured-evidence extraction run per page (processPage). It does NOT embed/classify — call
// Finalize for that. Splitting the two phases lets the driver overlap one chunk's embed (GPU) with
// the next chunk's fetch (network), so the GPU isn't idle while pages download.
//
// Concurrency model: a DOMAIN pool of cfg.Concurrency processDomain units, each fanning its pages
// into its own PageConcurrency page pool — "parallel domains" is the speed dial, and in-flight
// fetches are capped at Concurrency × PageConcurrency.
func FetchChunk(ctx context.Context, items []model.WorklistItem, getter fetch.RangeGetter, cfg ShardConfig) FetchedChunk {
	started := time.Now()
	s3Before := s3Stats(getter)
	byDomain := map[string][]model.WorklistItem{}
	var order []string
	for _, it := range items {
		if _, ok := byDomain[it.RootDomain]; !ok {
			order = append(order, it.RootDomain)
		}
		byDomain[it.RootDomain] = append(byDomain[it.RootDomain], it)
	}

	aggs := make([]*domainAgg, len(order))
	pagesByDomain := make([][]pageResult, len(order))
	conc := cfg.Concurrency
	if conc <= 0 {
		conc = 8
	}
	stats := &chunkStats{}
	var g errgroup.Group
	g.SetLimit(conc)
	for di, dom := range order {
		g.Go(func() error {
			aggs[di], pagesByDomain[di] = processDomain(ctx, getter, cfg, dom, byDomain[dom], stats)
			return nil
		})
	}
	_ = g.Wait()
	var successfulPages []pageResult
	for _, pages := range pagesByDomain {
		successfulPages = append(successfulPages, pages...)
	}

	n := atomic.LoadInt64(&stats.pages)
	if cfg.RunStats != nil {
		// Range run: fold this chunk into the process-wide sink and stay silent — the runner's
		// periodic aggregate line is the ONLY per-chunk output (spec §2 suppression).
		cfg.RunStats.add(n, atomic.LoadInt64(&stats.errs),
			atomic.LoadInt64(&stats.fetchNs), atomic.LoadInt64(&stats.parseNs), atomic.LoadInt64(&stats.techNs))
	} else {
		if n > 0 {
			errs := atomic.LoadInt64(&stats.errs)
			mode, _, _ := modeFlags(cfg)
			log.Printf("  timing/page (avg latency): fetch=%.0fms parse=%.1fms tech=%.1fms  errs=%d/%d  conc=%dx%d mode=%s",
				float64(stats.fetchNs)/float64(n)/1e6, float64(stats.parseNs)/float64(n)/1e6,
				float64(stats.techNs)/float64(n)/1e6, errs, n, conc, PageConcurrency, mode)
			if errs > 0 {
				log.Printf("  first fetch error: %s", stats.errSample)
			}
		}
		logS3Stats(ctx, getter, s3Before, time.Since(started), "fetch_chunk")
	}
	return FetchedChunk{aggs: aggs, pages: successfulPages, Pages: atomic.LoadInt64(&stats.pages), Errs: atomic.LoadInt64(&stats.errs)}
}

// Finalize embeds + classifies (industry) and builds the output rows from a fetched chunk. For the
// industry path this is the GPU-bound phase the driver overlaps with the next chunk's FetchChunk.
func Finalize(ctx context.Context, fc FetchedChunk, emb embedderIface, ref *model.Reference, protos *model.Prototypes, cfg ShardConfig) (ShardResult, error) {
	_, runEmbed, runTech := modeFlags(cfg)
	aggs := fc.aggs

	// Thin domain master: one identity row per fetched domain, written by every mode.
	var domains []output.DomainRow
	for _, a := range aggs {
		if a == nil || a.primaryURL == "" {
			continue
		}
		domains = append(domains, domainRow(cfg, a.rootDomain, a.primaryURL, a.primarySub))
	}

	var industries []output.IndustryRow
	var pageSignals []output.PageSignalRow
	var embeddings []output.EmbeddingRow
	var techRows []output.TechRow
	var identRows []output.IdentifierRow
	var jsonldRows []output.JSONLDRow
	var contacts []output.ContactRow
	var securityRows []output.SecurityRow
	var pageMetaRows []output.PageMetaRow

	if runEmbed {
		var idx []int
		var texts []string
		for i, a := range aggs {
			if a != nil && a.hasPrimary {
				idx = append(idx, i)
				texts = append(texts, a.primaryText)
			}
		}
		tEmbed := time.Now()
		var vecs [][]float32
		if len(texts) > 0 {
			var err error
			if vecs, err = emb.Embed(texts, classifyInstruction); err != nil {
				return ShardResult{}, err
			}
		}
		embedDur := time.Since(tEmbed)

		// Classify is a per-domain cosine over the whole NACE matrix (1k×4k) — fan it across all
		// cores. Each worker writes its own non-overlapping index range (no shared write).
		tClass := time.Now()
		indByI := make([][]output.IndustryRow, len(idx))
		sigByI := make([]output.PageSignalRow, len(idx))
		cw := runtime.NumCPU()
		if cw > len(idx) {
			cw = len(idx)
		}
		var cwg sync.WaitGroup
		for w := 0; w < cw; w++ {
			cwg.Add(1)
			go func(w int) {
				defer cwg.Done()
				for vi := w; vi < len(idx); vi += cw {
					a := aggs[idx[vi]]
					var res model.DomainResult
					if label := classify.MatchSignal(a.primaryText); label != "" {
						res = model.DomainResult{PageType: label, NaceMethod: "keyword"}
					} else {
						res = classify.Classify(vecs[vi], ref, protos)
					}
					indByI[vi] = industryRows(cfg, a.rootDomain, a.primaryURL, res)
					sigByI[vi] = pageSignalRow(cfg, a.rootDomain, a.primaryURL, a.primarySub, res)
				}
			}(w)
		}
		cwg.Wait()
		for vi := range idx {
			industries = append(industries, indByI[vi]...)
			pageSignals = append(pageSignals, sigByI[vi])
		}
		// Keep the raw vectors: they are the expensive GPU artifact, and "both" streams them to the
		// embedding tree exactly like the industry stream path does.
		for vi, i := range idx {
			a := aggs[i]
			embeddings = append(embeddings, embeddingRow(cfg, streamItem{
				root: a.rootDomain, url: a.primaryURL, sub: a.primarySub, text: a.primaryText,
				warc: a.primaryWarc, off: a.primaryOff, length: a.primaryLen,
			}, vecs[vi]))
		}
		if len(idx) > 0 {
			log.Printf("  embed=%.1fs classify=%.1fs for %d domains (%d classify workers)",
				embedDur.Seconds(), time.Since(tClass).Seconds(), len(idx), cw)
		}
	}

	if runTech {
		contactSeen := map[string]bool{}
		addContact := func(rootDomain, pageURL, contactType, value, source string) {
			value = strings.TrimSpace(value)
			if value == "" {
				return
			}
			key := rootDomain + "\x00" + contactType + "\x00" + value
			if contactSeen[key] {
				return
			}
			contactSeen[key] = true
			contacts = append(contacts, contactRow(cfg, rootDomain, pageURL, contactType, value, source))
		}

		for _, page := range fc.pages {
			it := page.source
			offset, length := uint64(it.Offset), uint64(it.Length)
			for _, technology := range page.tech {
				techRows = append(techRows, output.TechRow{
					CrawlID: cfg.CrawlID, RootDomain: it.RootDomain, PageURL: it.URL, Subdomain: page.sub,
					WarcIndex: it.WarcIndex, WarcFilename: it.WarcFilename,
					WarcRecordOffset: offset, WarcRecordLength: length,
					Technology: technology.Name, Category: technology.Category,
					Version: technology.Version, Confidence: technology.Confidence,
					SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
				})
			}
			for _, entity := range page.jsonld {
				jsonldRows = append(jsonldRows, output.JSONLDRow{
					CrawlID: cfg.CrawlID, RootDomain: it.RootDomain, PageURL: it.URL, Subdomain: page.sub,
					WarcIndex: it.WarcIndex, WarcFilename: it.WarcFilename,
					WarcRecordOffset: offset, WarcRecordLength: length,
					ScriptIndex: entity.ScriptIndex, EntityPath: entity.EntityPath,
					EntityID: entity.ID, EntityTypes: entity.Types,
					IsOrganization: b2u8(entity.IsOrganization),
					Name:           entity.Name, LegalName: entity.LegalName, Description: entity.Description,
					EntityURL: entity.URL, Logo: entity.Logo, Email: entity.Email,
					Telephone: entity.Phone, SameAs: entity.SameAs, Country: entity.Country,
					FoundingYear: entity.FoundingYear, EmployeeCount: entity.EmployeeCount,
					EntityJSON:  entity.RawJSON,
					SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
				})
				if entity.IsOrganization {
					addContact(it.RootDomain, it.URL, "email", entity.Email, "jsonld")
					addContact(it.RootDomain, it.URL, "phone", extract.NormalizePhone(entity.Phone, it.RootDomain), "jsonld")
					for _, sameAs := range entity.SameAs {
						addContact(it.RootDomain, it.URL, "social", sameAs, "jsonld")
					}
				}
			}
			addContact(it.RootDomain, it.URL, "email", page.microdata.Email, "microdata")
			addContact(it.RootDomain, it.URL, "phone", extract.NormalizePhone(page.microdata.Phone, it.RootDomain), "microdata")
			for _, sameAs := range page.microdata.SameAs {
				addContact(it.RootDomain, it.URL, "social", sameAs, "microdata")
			}
			for _, email := range page.emails {
				addContact(it.RootDomain, it.URL, "email", email, "regex")
			}
		}
		for _, a := range aggs {
			if a == nil || a.primaryURL == "" {
				continue
			}
			for _, id := range a.identifiers {
				valid := uint8(0)
				if id.Valid {
					valid = 1
				}
				identRows = append(identRows, output.IdentifierRow{
					CrawlID: cfg.CrawlID, RootDomain: a.rootDomain, URL: a.primaryURL, Subdomain: a.primarySub,
					IDType: id.Type, IDValue: id.Value, Valid: valid, Source: id.Source,
					SourceURL: a.primaryURL, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
				})
			}
			// security posture + head signals: the primary page's captured headers/meta (empty if the
			// homepage itself failed to fetch — then this domain has no security/meta row).
			if len(a.security) > 0 {
				securityRows = append(securityRows, output.SecurityRow{
					CrawlID: cfg.CrawlID, RootDomain: a.rootDomain, SourceURL: a.primaryURL,
					Headers: a.security, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
				})
			}
			if a.meta.Title != "" || len(a.meta.Meta) > 0 || a.meta.Canonical != "" || len(a.meta.Hreflang) > 0 || len(a.jsonldTypes) > 0 {
				pageMetaRows = append(pageMetaRows, output.PageMetaRow{
					CrawlID: cfg.CrawlID, RootDomain: a.rootDomain, SourceURL: a.primaryURL,
					Title: a.meta.Title, Meta: a.meta.Meta, Canonical: a.meta.Canonical,
					Hreflang: a.meta.Hreflang, JSONLDTypes: a.jsonldTypes, Charset: a.meta.Charset,
					SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
				})
			}
		}
	}
	return ShardResult{Domains: domains, Industries: industries, PageSignals: pageSignals,
		Embeddings: embeddings, Tech: techRows,
		Identifiers: identRows, JSONLD: jsonldRows, Contacts: contacts,
		Security: securityRows, PageMeta: pageMetaRows}, nil
}

// streamItem is one fetched primary page handed from the fetch stage to an embed worker.
type streamItem struct {
	di          int
	root, url   string
	sub         string
	text        string
	warc        string // archived page location — carried to the embedding for re-fetch
	off, length int64
}

// ProcessIndustryStream runs the WHOLE industry shard as a continuous fetch→embed pipeline: fetch
// goroutines stream each domain's primary page to a pool of embed workers that keep
// cfg.EmbedConcurrency requests in flight at all times. Unlike the chunked path it never does
// "embed all, then wait" — so the GPU is fed continuously and doesn't drain between bursts.
func ProcessIndustryStream(ctx context.Context, items []model.WorklistItem, getter fetch.RangeGetter,
	emb embedderIface, ref *model.Reference, protos *model.Prototypes, cfg ShardConfig) (ShardResult, error) {

	byDomain := map[string][]model.WorklistItem{}
	var order []string
	for _, it := range items {
		if _, ok := byDomain[it.RootDomain]; !ok {
			order = append(order, it.RootDomain)
		}
		byDomain[it.RootDomain] = append(byDomain[it.RootDomain], it)
	}
	domains := make([]output.DomainRow, len(order))     // written by di; empty rows filtered at end
	indByDi := make([][]output.IndustryRow, len(order)) // industries per domain, written by di
	sigByDi := make([]output.PageSignalRow, len(order)) // page signals per domain, written by di
	embByDi := make([]output.EmbeddingRow, len(order))  // raw page vector per domain, written by di

	conc := cfg.Concurrency
	if conc <= 0 {
		conc = 8
	}
	embConc := cfg.EmbedConcurrency
	if embConc <= 0 {
		embConc = 32
	}
	embBatch := cfg.EmbedBatch
	if embBatch <= 0 {
		embBatch = 16
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := make(chan streamItem, embConc*embBatch)
	var fetchNs, parseNs, pageCount, errCount, embedNs, embedReqs, done int64
	var errSample string
	var errOnce, embedErrOnce sync.Once
	var embedErr error

	// progress ticker — suppressed in a range run (cfg.RunStats set): the runner's cumulative line
	// replaces it, and 8 of these interleaving is exactly the noise being removed.
	stop := make(chan struct{})
	start := time.Now()
	s3Before := s3Stats(getter)
	if cfg.RunStats == nil {
		go func() {
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()
			last, lastT := int64(0), start
			for {
				select {
				case <-stop:
					return
				case now := <-t.C:
					n := atomic.LoadInt64(&done)
					inst := float64(n-last) / now.Sub(lastT).Seconds() // rate over the last tick (not cold-start avg)
					log.Printf("  industry stream: %d/%d domains (%.0f/s now, %.0f/s avg, conc=%d embed=%d)",
						n, len(order), inst, float64(n)/time.Since(start).Seconds(), conc, embConc)
					last, lastT = n, now
				}
			}
		}()
	}

	// embed workers: each keeps one request in flight; embConc of them => embConc concurrent.
	var ewg sync.WaitGroup
	for w := 0; w < embConc; w++ {
		ewg.Add(1)
		go func() {
			defer ewg.Done()
			var batch []streamItem
			texts := make([]string, 0, embBatch)
			flush := func() {
				if len(batch) == 0 {
					return
				}
				texts = texts[:0]
				for k := range batch {
					texts = append(texts, batch[k].text)
				}
				t0 := time.Now()
				vecs, err := emb.Embed(texts, classifyInstruction)
				atomic.AddInt64(&embedNs, time.Since(t0).Nanoseconds())
				atomic.AddInt64(&embedReqs, 1)
				if err != nil {
					embedErrOnce.Do(func() { embedErr = err })
					cancel()
					batch = batch[:0]
					return
				}
				for k := range batch {
					b := batch[k]
					embByDi[b.di] = embeddingRow(cfg, b, vecs[k]) // keep the raw vector + page metadata
					if cfg.EmbedOnly {
						continue // embed-only: no NACE classify, no domain/industry/signal rows
					}
					var res model.DomainResult
					if label := classify.MatchSignal(b.text); label != "" {
						res = model.DomainResult{PageType: label, NaceMethod: "keyword"}
					} else {
						res = classify.Classify(vecs[k], ref, protos)
					}
					domains[b.di] = domainRow(cfg, b.root, b.url, b.sub)
					indByDi[b.di] = industryRows(cfg, b.root, b.url, res)
					sigByDi[b.di] = pageSignalRow(cfg, b.root, b.url, b.sub, res)
				}
				atomic.AddInt64(&done, int64(len(batch)))
				batch = batch[:0]
			}
			for it := range ch {
				batch = append(batch, it)
				if len(batch) >= embBatch {
					flush()
				}
			}
			flush()
		}()
	}

	// fetch workers: one primary page per domain -> stream to the embed workers.
	sem := make(chan struct{}, conc)
	var fwg sync.WaitGroup
	for di, dom := range order {
		fwg.Add(1)
		go func(di int, dom string) {
			defer fwg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			for _, wit := range byDomain[dom] {
				if !wit.Primary {
					continue
				}
				t0 := time.Now()
				fctx, fcancel := context.WithTimeout(cctx, 30*time.Second)
				headers, body, err := fetch.FetchRecord(fctx, getter, ccBucket, wit.WarcFilename, wit.Offset, wit.Length)
				fcancel()
				atomic.AddInt64(&fetchNs, time.Since(t0).Nanoseconds())
				atomic.AddInt64(&pageCount, 1)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					errOnce.Do(func() { errSample = err.Error() })
					return
				}
				decoded, _ := parse.DecodeHTML(body, contentTypeOf(headers))
				t1 := time.Now()
				text := parse.MainText(decoded, wit.URL)
				atomic.AddInt64(&parseNs, time.Since(t1).Nanoseconds())
				select {
				case ch <- streamItem{di: di, root: dom, url: wit.URL, sub: subdomainOf(wit.URL, dom), text: text,
					warc: wit.WarcFilename, off: wit.Offset, length: wit.Length}:
				case <-cctx.Done():
				}
				return
			}
		}(di, dom)
	}
	fwg.Wait()
	close(ch)
	ewg.Wait()
	close(stop)
	if embedErr != nil {
		return ShardResult{}, embedErr
	}

	if cfg.RunStats != nil {
		// Range run: fold this shard's page counters into the sink (no tech in this path) and stay
		// silent, leaving the runner's cumulative line as the only progress output.
		cfg.RunStats.add(atomic.LoadInt64(&pageCount), atomic.LoadInt64(&errCount),
			atomic.LoadInt64(&fetchNs), atomic.LoadInt64(&parseNs), 0)
	} else {
		logS3Stats(ctx, getter, s3Before, time.Since(start), "industry_stream")
		if n := atomic.LoadInt64(&pageCount); n > 0 {
			reqs := atomic.LoadInt64(&embedReqs)
			avgEmbed := 0.0
			if reqs > 0 {
				avgEmbed = float64(embedNs) / float64(reqs) / 1e6
			}
			log.Printf("  industry: fetch=%.0fms parse=%.1fms/page, embed=%.0fms/req over %d reqs, errs=%d/%d conc=%d embed=%d",
				float64(fetchNs)/float64(n)/1e6, float64(parseNs)/float64(n)/1e6, avgEmbed, reqs,
				atomic.LoadInt64(&errCount), n, conc, embConc)
			if errCount > 0 {
				log.Printf("  first fetch error: %s", errSample)
			}
		}
	}
	out := domains[:0]
	var industries []output.IndustryRow
	var pageSignals []output.PageSignalRow
	var embeddings []output.EmbeddingRow
	for di := range domains {
		if cfg.EmbedOnly { // embed-only fills only embByDi, never domains
			if embByDi[di].RootDomain != "" {
				embeddings = append(embeddings, embByDi[di])
			}
			continue
		}
		if domains[di].RootDomain != "" {
			out = append(out, domains[di])
			industries = append(industries, indByDi[di]...)
			pageSignals = append(pageSignals, sigByDi[di])
			embeddings = append(embeddings, embByDi[di])
		}
	}
	return ShardResult{Domains: out, Industries: industries, PageSignals: pageSignals, Embeddings: embeddings,
		Pages: atomic.LoadInt64(&pageCount), Errs: atomic.LoadInt64(&errCount)}, nil
}

// ProcessShard processes one worklist chunk end-to-end (fetch then finalize). Use FetchChunk +
// Finalize directly to pipeline fetch and embed across chunks.
//
//	"industry": fetch each domain's primary page -> embed -> classify -> domain rows
//	"tech":     fetch every page -> Wappalyzer + identifiers/JSON-LD -> per-page rows
//	"both"  :   both in a single fetch pass (default)
func ProcessShard(ctx context.Context, items []model.WorklistItem, getter fetch.RangeGetter, emb embedderIface,
	ref *model.Reference, protos *model.Prototypes, cfg ShardConfig) (ShardResult, error) {
	return Finalize(ctx, FetchChunk(ctx, items, getter, cfg), emb, ref, protos, cfg)
}
