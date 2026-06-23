package main

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"time"
)

const classifyInstruction = "Classify the business into its industry category"

// WorklistItem is one row of the index-driven worklist (top-K pages per domain).
type WorklistItem struct {
	RootDomain, URL, WarcFilename string
	Offset, Length                int64
	Primary                       bool // rn == 1 (the shallowest page; gets embedded)
}

type embedderIface interface {
	Embed(texts []string, instruction string) ([][]float32, error)
}

type ShardConfig struct {
	CrawlID, SourceRunID string
	ResolvedAt           time.Time
	Concurrency          int
}

type domainAgg struct {
	rootDomain, primaryURL, primarySub, primaryText string
	emails                                          []string
	tech                                            []Technology
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

// ProcessShard fetches+parses+fingerprints every page (all K) across all cores, embeds
// each domain's primary text in one batch, then classifies (keyword signal else cosine
// gates) into one DomainResult per domain with the union of its pages' technologies.
func ProcessShard(ctx context.Context, items []WorklistItem, getter rangeGetter, emb embedderIface,
	ref *Reference, protos *Prototypes, cfg ShardConfig) ([]DomainRow, []TechRow, error) {

	byDomain := map[string][]WorklistItem{}
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
			for _, it := range byDomain[dom] {
				headers, body, err := FetchRecord(ctx, getter, ccBucket, it.WarcFilename, it.Offset, it.Length)
				if err != nil {
					continue // skip a bad page; the domain still emits if its primary survived
				}
				text, emails, _ := ParseHTML(string(body))
				for _, t := range DetectTech(headers, body) {
					if !techSeen[t.Name] {
						techSeen[t.Name] = true
						agg.tech = append(agg.tech, t)
					}
				}
				if it.Primary && !agg.hasPrimary {
					agg.hasPrimary = true
					agg.primaryURL = it.URL
					agg.primarySub = subdomainOf(it.URL, dom)
					agg.primaryText = text
					agg.emails = emails
				}
			}
			aggs[di] = agg
		}(di, dom)
	}
	wg.Wait()

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
			return nil, nil, err
		}
	}

	var domains []DomainRow
	var techRows []TechRow
	for vi, i := range idx {
		a := aggs[i]
		var res DomainResult
		if label := MatchSignal(a.primaryText); label != "" {
			res = DomainResult{PageType: label, NaceMethod: "keyword"}
		} else {
			res = Classify(vecs[vi], ref, protos)
		}
		conf := uint8(0)
		if res.NaceConfident {
			conf = 1
		}
		domains = append(domains, DomainRow{
			CrawlID: cfg.CrawlID, URL: a.primaryURL, RootDomain: a.rootDomain, Subdomain: a.primarySub,
			Emails: a.emails, EmailCount: uint32(len(a.emails)),
			PageType: res.PageType, PageTypeScore: res.PageTypeScore,
			NaceCode: res.NaceCode, NaceLabel: res.NaceLabel, NaceDivision: res.NaceDivision,
			NaceConfident: conf, NaceMargin: res.NaceMargin, NaceScore: res.NaceScore, NaceMethod: res.NaceMethod,
			Top3Codes: res.Top3Codes, Top3Labels: res.Top3Labels, Top3Scores: res.Top3Scores,
			SourceURL: a.primaryURL, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
		})
		for _, t := range a.tech {
			techRows = append(techRows, TechRow{
				CrawlID: cfg.CrawlID, URL: a.primaryURL, RootDomain: a.rootDomain, Subdomain: a.primarySub,
				Technology: t.Name, Category: t.Category, Version: t.Version, Confidence: 100,
				SourceURL: a.primaryURL, SourceRunID: cfg.SourceRunID, ResolvedAt: cfg.ResolvedAt,
			})
		}
	}
	return domains, techRows, nil
}
