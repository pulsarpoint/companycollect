package main

import (
	"context"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cc-enrich-worker/internal/fetch"
	"cc-enrich-worker/internal/model"
	"cc-enrich-worker/internal/output"
	"cc-enrich-worker/internal/parse"
	"cc-enrich-worker/internal/tech"
)

const classifyInstruction = "Classify the business into its industry category"

type embedderIface interface {
	Embed(texts []string, instruction string) ([][]float32, error)
}

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
	TechMaxBytes         int    // body cap fed to Wappalyzer; 0 => techMaxBytes default
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

// ProcessShard processes one worklist shard. cfg.Mode selects the work:
//
//	"industry": fetch each domain's primary page -> embed -> classify -> domain rows
//	            (needs the ClickHouse reference + embedder; no Wappalyzer)
//	"tech":     fetch every page -> Wappalyzer -> per-domain unioned tech rows
//	            (no ClickHouse, no embedder)
//	"both"  :   both in a single fetch pass (default)
func ProcessShard(ctx context.Context, items []model.WorklistItem, getter fetch.RangeGetter, emb embedderIface,
	ref *model.Reference, protos *model.Prototypes, cfg ShardConfig) (ShardResult, error) {

	mode := cfg.Mode
	if mode == "" {
		mode = "both"
	}
	runEmbed := mode != "tech"
	runTech := mode != "industry"

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
					prof, structIDs := ExtractProfile(techBody) // schema.org Organization JSON-LD
					for _, id := range structIDs {
						if !idSeen[id.Value] {
							idSeen[id.Value] = true
							agg.identifiers = append(agg.identifiers, id)
						}
					}
					for _, id := range ExtractLEIs(techBody) { // jsonld-regex + text LEIs
						if !idSeen[id.Value] {
							idSeen[id.Value] = true
							agg.identifiers = append(agg.identifiers, id)
						}
					}
					for _, id := range ExtractVATs(techBody) { // EU VAT ids (format + checksum)
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
		log.Printf("  timing/page (avg latency): fetch=%.0fms parse=%.1fms tech=%.1fms  errs=%d/%d  conc=%d mode=%s",
			float64(fetchNs)/float64(n)/1e6, float64(parseNs)/float64(n)/1e6,
			float64(techNs)/float64(n)/1e6, errs, n, conc, mode)
		if errs > 0 {
			log.Printf("  first fetch error: %s", errSample)
		}
	}

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
		var vecs [][]float32
		if len(texts) > 0 {
			var err error
			if vecs, err = emb.Embed(texts, classifyInstruction); err != nil {
				return ShardResult{}, err
			}
		}
		for vi, i := range idx {
			a := aggs[i]
			var res model.DomainResult
			if label := MatchSignal(a.primaryText); label != "" {
				res = model.DomainResult{PageType: label, NaceMethod: "keyword"}
			} else {
				res = Classify(vecs[vi], ref, protos)
			}
			conf := uint8(0)
			if res.NaceConfident {
				conf = 1
			}
			domains = append(domains, output.DomainRow{
				CrawlID: cfg.CrawlID, URL: a.primaryURL, RootDomain: a.rootDomain, Subdomain: a.primarySub,
				Emails: a.emails, EmailCount: uint32(len(a.emails)),
				PageType: res.PageType, PageTypeScore: res.PageTypeScore,
				NaceCode: res.NaceCode, NaceLabel: res.NaceLabel, NaceDivision: res.NaceDivision,
				NaceConfident: conf, NaceConfidence: res.NaceConfidence,
				NaceMargin: res.NaceMargin, NaceScore: res.NaceScore, NaceMethod: res.NaceMethod,
				Top3Codes: res.Top3Codes, Top3Labels: res.Top3Labels, Top3Scores: res.Top3Scores,
				SourceURL: a.primaryURL, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
			})
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
