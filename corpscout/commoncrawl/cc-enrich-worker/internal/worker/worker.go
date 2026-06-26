package worker

import (
	"context"
	"log"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cc-enrich-worker/internal/classify"
	"cc-enrich-worker/internal/extract"
	"cc-enrich-worker/internal/fetch"
	"cc-enrich-worker/internal/model"
	"cc-enrich-worker/internal/output"
	"cc-enrich-worker/internal/parse"
	"cc-enrich-worker/internal/tech"
)

const (
	ccBucket = "commoncrawl"
)

const classifyInstruction = "Classify the business into its industry category"

type embedderIface = classify.Embedder

// ShardResult bundles the per-pass outputs (domains from industry; tech/identifiers/
// profiles from tech). Each maps to its own ClickHouse table.
type ShardResult struct {
	Domains     []output.DomainRow
	Tech        []output.TechRow
	Identifiers []output.IdentifierRow
	Profiles    []output.ProfileRow
}

type ShardConfig struct {
	CrawlID, SourceRunID string
	ResolvedAt           time.Time
	Concurrency          int
	EmbedConcurrency     int    // industry stream: embed requests kept continuously in flight
	EmbedBatch           int    // industry stream: texts per embed request
	TechMaxBytes         int    // body cap fed to Wappalyzer; 0 => no cap (full body)
	Mode                 string // "industry" | "tech" | "both" (default "both")
}

type domainAgg struct {
	rootDomain, primaryURL, primarySub, primaryText string
	emails                                          []string
	tech                                            []model.Technology
	identifiers                                     []model.Identifier
	profile                                         model.CompanyProfile
	hasPrimary                                      bool
}

// FetchedChunk holds the per-domain aggregates produced by FetchChunk, ready for Finalize.
// (domainAgg is unexported, so callers pass this opaque value between the two phases.)
type FetchedChunk struct{ aggs []*domainAgg }

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

func modeFlags(cfg ShardConfig) (mode string, runEmbed, runTech bool) {
	mode = cfg.Mode
	if mode == "" {
		mode = "both"
	}
	return mode, mode != "tech", mode != "industry"
}

// FetchChunk downloads + parses every page of the chunk into per-domain aggregates; tech detection
// and identifier/profile extraction run inline here. It does NOT embed/classify — call Finalize for
// that. Splitting the two phases lets the driver overlap one chunk's embed (GPU) with the next
// chunk's fetch (network), so the GPU isn't idle while pages download.
func FetchChunk(ctx context.Context, items []model.WorklistItem, getter fetch.RangeGetter, cfg ShardConfig) FetchedChunk {
	_, runEmbed, runTech := modeFlags(cfg)

	byDomain := map[string][]model.WorklistItem{}
	var order []string
	for _, it := range items {
		if _, ok := byDomain[it.RootDomain]; !ok {
			order = append(order, it.RootDomain)
		}
		byDomain[it.RootDomain] = append(byDomain[it.RootDomain], it)
	}

	aggs := make([]*domainAgg, len(order))
	conc := cfg.Concurrency
	if conc <= 0 {
		conc = 8
	}
	var fetchNs, parseNs, techNs, pageCount, errCount int64 // per-stage timing (diagnostics)
	var errSample string
	var errOnce sync.Once
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	for di, dom := range order {
		wg.Add(1)
		go func(di int, dom string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			agg := &domainAgg{rootDomain: dom}
			techSeen := map[string]bool{}
			idSeen := map[string]bool{}
			for _, it := range byDomain[dom] {
				t0 := time.Now()
				fctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				headers, body, err := fetch.FetchRecord(fctx, getter, ccBucket, it.WarcFilename, it.Offset, it.Length)
				cancel()
				atomic.AddInt64(&fetchNs, time.Since(t0).Nanoseconds())
				atomic.AddInt64(&pageCount, 1)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					errOnce.Do(func() { errSample = err.Error() })
					continue // skip a bad/slow page; the domain still emits if its primary survived
				}
				if agg.primaryURL == "" { // first surviving page keys this domain's rows
					agg.primaryURL = it.URL
					agg.primarySub = subdomainOf(it.URL, dom)
				}
				if runTech {
					t2 := time.Now()
					techBody := body
					if limit := cfg.TechMaxBytes; limit > 0 && len(techBody) > limit {
						techBody = techBody[:limit] // cap regex input; tech signals live in headers + early body
					}
					detected := tech.DetectTech(headers, techBody)
					atomic.AddInt64(&techNs, time.Since(t2).Nanoseconds())
					for _, t := range detected {
						if !techSeen[t.Name] {
							techSeen[t.Name] = true
							agg.tech = append(agg.tech, t)
						}
					}
					prof, structIDs := extract.ExtractProfile(techBody) // schema.org Organization JSON-LD
					for _, id := range structIDs {
						if !idSeen[id.Value] {
							idSeen[id.Value] = true
							agg.identifiers = append(agg.identifiers, id)
						}
					}
					for _, id := range extract.ExtractLEIs(techBody) { // jsonld-regex + text LEIs
						if !idSeen[id.Value] {
							idSeen[id.Value] = true
							agg.identifiers = append(agg.identifiers, id)
						}
					}
					for _, id := range extract.ExtractVATs(techBody) { // EU VAT ids (format + checksum)
						if !idSeen[id.Value] {
							idSeen[id.Value] = true
							agg.identifiers = append(agg.identifiers, id)
						}
					}
					if agg.profile.Empty() && !prof.Empty() {
						agg.profile = prof
					}
				}
				if runEmbed && it.Primary && !agg.hasPrimary {
					t1 := time.Now()
					text, emails, _ := parse.ParseHTML(string(body))
					atomic.AddInt64(&parseNs, time.Since(t1).Nanoseconds())
					agg.hasPrimary = true
					agg.primaryText = text
					agg.emails = emails
				}
			}
			aggs[di] = agg
		}(di, dom)
	}
	wg.Wait()
	if n := atomic.LoadInt64(&pageCount); n > 0 {
		errs := atomic.LoadInt64(&errCount)
		mode, _, _ := modeFlags(cfg)
		log.Printf("  timing/page (avg latency): fetch=%.0fms parse=%.1fms tech=%.1fms  errs=%d/%d  conc=%d mode=%s",
			float64(fetchNs)/float64(n)/1e6, float64(parseNs)/float64(n)/1e6,
			float64(techNs)/float64(n)/1e6, errs, n, conc, mode)
		if errs > 0 {
			log.Printf("  first fetch error: %s", errSample)
		}
	}
	return FetchedChunk{aggs: aggs}
}

// Finalize embeds + classifies (industry) and builds the output rows from a fetched chunk. For the
// industry path this is the GPU-bound phase the driver overlaps with the next chunk's FetchChunk.
func Finalize(ctx context.Context, fc FetchedChunk, emb embedderIface, ref *model.Reference, protos *model.Prototypes, cfg ShardConfig) (ShardResult, error) {
	_, runEmbed, runTech := modeFlags(cfg)
	aggs := fc.aggs

	var domains []output.DomainRow
	var techRows []output.TechRow
	var identRows []output.IdentifierRow
	var profileRows []output.ProfileRow

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
		// cores instead of one. Each worker writes its own non-overlapping range of domains.
		tClass := time.Now()
		domains = make([]output.DomainRow, len(idx))
		cw := runtime.NumCPU()
		if cw > len(idx) {
			cw = len(idx)
		}
		var cwg sync.WaitGroup
		for w := 0; w < cw; w++ {
			cwg.Add(1)
			go func(w int) {
				defer cwg.Done()
				for vi := w; vi < len(idx); vi += cw { // stride partition -> balanced, no shared write
					a := aggs[idx[vi]]
					var res model.DomainResult
					if label := classify.MatchSignal(a.primaryText); label != "" {
						res = model.DomainResult{PageType: label, NaceMethod: "keyword"}
					} else {
						res = classify.Classify(vecs[vi], ref, protos)
					}
					conf := uint8(0)
					if res.NaceConfident {
						conf = 1
					}
					domains[vi] = output.DomainRow{
						CrawlID: cfg.CrawlID, URL: a.primaryURL, RootDomain: a.rootDomain, Subdomain: a.primarySub,
						Emails: a.emails, EmailCount: uint32(len(a.emails)),
						PageType: res.PageType, PageTypeScore: res.PageTypeScore,
						NaceCode: res.NaceCode, NaceLabel: res.NaceLabel, NaceDivision: res.NaceDivision,
						NaceConfident: conf, NaceConfidence: res.NaceConfidence,
						NaceMargin: res.NaceMargin, NaceScore: res.NaceScore, NaceMethod: res.NaceMethod,
						Top3Codes: res.Top3Codes, Top3Labels: res.Top3Labels, Top3Scores: res.Top3Scores,
						SourceURL: a.primaryURL, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
					}
				}
			}(w)
		}
		cwg.Wait()
		if len(idx) > 0 {
			log.Printf("  embed=%.1fs classify=%.1fs for %d domains (%d classify workers)",
				embedDur.Seconds(), time.Since(tClass).Seconds(), len(idx), cw)
		}
	}

	if runTech {
		for _, a := range aggs {
			if a == nil || a.primaryURL == "" {
				continue
			}
			for _, t := range a.tech {
				techRows = append(techRows, output.TechRow{
					CrawlID: cfg.CrawlID, URL: a.primaryURL, RootDomain: a.rootDomain, Subdomain: a.primarySub,
					Technology: t.Name, Category: t.Category, Version: t.Version, Confidence: 100,
					SourceURL: a.primaryURL, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
				})
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
			if p := a.profile; !p.Empty() {
				profileRows = append(profileRows, output.ProfileRow{
					CrawlID: cfg.CrawlID, RootDomain: a.rootDomain, URL: a.primaryURL, Subdomain: a.primarySub,
					Name: p.Name, Description: p.Description, Logo: p.Logo, Country: p.Country,
					Email: p.Email, Phone: p.Phone, FoundingYear: p.FoundingYear, EmployeeCount: p.EmployeeCount,
					SameAs: p.SameAs, Source: "jsonld",
					SourceURL: a.primaryURL, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
				})
			}
		}
	}
	return ShardResult{Domains: domains, Tech: techRows, Identifiers: identRows, Profiles: profileRows}, nil
}

// streamItem is one fetched primary page handed from the fetch stage to an embed worker.
type streamItem struct {
	di        int
	root, url string
	sub       string
	text      string
	emails    []string
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
	domains := make([]output.DomainRow, len(order)) // written by di; empty rows filtered at end

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

	// progress ticker
	stop := make(chan struct{})
	start := time.Now()
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				n := atomic.LoadInt64(&done)
				log.Printf("  industry stream: %d/%d domains (%.1f domains/s, conc=%d embed=%d)",
					n, len(order), float64(n)/time.Since(start).Seconds(), conc, embConc)
			}
		}
	}()

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
					var res model.DomainResult
					if label := classify.MatchSignal(b.text); label != "" {
						res = model.DomainResult{PageType: label, NaceMethod: "keyword"}
					} else {
						res = classify.Classify(vecs[k], ref, protos)
					}
					conf := uint8(0)
					if res.NaceConfident {
						conf = 1
					}
					domains[b.di] = output.DomainRow{
						CrawlID: cfg.CrawlID, URL: b.url, RootDomain: b.root, Subdomain: b.sub,
						Emails: b.emails, EmailCount: uint32(len(b.emails)),
						PageType: res.PageType, PageTypeScore: res.PageTypeScore,
						NaceCode: res.NaceCode, NaceLabel: res.NaceLabel, NaceDivision: res.NaceDivision,
						NaceConfident: conf, NaceConfidence: res.NaceConfidence,
						NaceMargin: res.NaceMargin, NaceScore: res.NaceScore, NaceMethod: res.NaceMethod,
						Top3Codes: res.Top3Codes, Top3Labels: res.Top3Labels, Top3Scores: res.Top3Scores,
						SourceURL: b.url, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
					}
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
				_, body, err := fetch.FetchRecord(fctx, getter, ccBucket, wit.WarcFilename, wit.Offset, wit.Length)
				fcancel()
				atomic.AddInt64(&fetchNs, time.Since(t0).Nanoseconds())
				atomic.AddInt64(&pageCount, 1)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					errOnce.Do(func() { errSample = err.Error() })
					return
				}
				t1 := time.Now()
				text, emails, _ := parse.ParseHTML(string(body))
				atomic.AddInt64(&parseNs, time.Since(t1).Nanoseconds())
				select {
				case ch <- streamItem{di: di, root: dom, url: wit.URL, sub: subdomainOf(wit.URL, dom), text: text, emails: emails}:
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
	out := domains[:0]
	for _, d := range domains {
		if d.RootDomain != "" {
			out = append(out, d)
		}
	}
	return ShardResult{Domains: out}, nil
}

// ProcessShard processes one worklist chunk end-to-end (fetch then finalize). Use FetchChunk +
// Finalize directly to pipeline fetch and embed across chunks.
//
//	"industry": fetch each domain's primary page -> embed -> classify -> domain rows
//	"tech":     fetch every page -> Wappalyzer + identifiers/profile -> per-domain rows
//	"both"  :   both in a single fetch pass (default)
func ProcessShard(ctx context.Context, items []model.WorklistItem, getter fetch.RangeGetter, emb embedderIface,
	ref *model.Reference, protos *model.Prototypes, cfg ShardConfig) (ShardResult, error) {
	return Finalize(ctx, FetchChunk(ctx, items, getter, cfg), emb, ref, protos, cfg)
}
