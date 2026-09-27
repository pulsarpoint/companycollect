// Package assemble turns a curated definition plus this run's feed outcomes into
// a provider-recon/v1 document.
package assemble

import (
	"errors"
	"net/netip"
	"sort"
	"time"

	"provider_recon/internal/definitions"
	"provider_recon/internal/feeds"
	"provider_recon/internal/model"
)

// FeedOutcome is one collector run: a result or an error.
type FeedOutcome struct {
	Result feeds.Result
	Err    error
}

const (
	curatedConfidence  = 1.0
	officialConfidence = 1.0
	bgpConfidence      = 0.8
)

// bgpCollectors publish announcements, not operator-published ranges.
var bgpCollectors = map[string]bool{"ripestat_announced": true}

// Build assembles the document. A failed feed keeps prev's ranges for that
// feed (status stale); with no previous success its status is failed and only
// curated evidence remains. It never returns a document that lost ranges
// because a feed broke.
func Build(def definitions.Definition, outcomes map[string]FeedOutcome, prev *model.Document, now time.Time) (model.Document, error) {
	doc := model.Document{
		Version: model.ContractVersion, Slug: def.Slug, DisplayName: def.DisplayName,
		Category: def.Category, Website: def.Website, Country: def.Country,
		Aliases: def.Aliases, ProviderKeys: def.ProviderKeys,
		Collection: model.Collection{CollectedAt: now, Collectors: map[string]model.CollectorStatus{}},
	}
	index := map[string]int{}
	for _, s := range def.Services {
		index[s.Key] = len(doc.Services)
		doc.Services = append(doc.Services, curatedService(s))
	}
	for _, ref := range def.Feeds {
		id := ref.ID()
		outcome, ran := outcomes[id]
		if !ran {
			outcome = FeedOutcome{Err: errors.New("collector did not run")}
		}
		if outcome.Err == nil {
			doc.Collection.Collectors[id] = applyFeed(&doc, index, ref, outcome.Result, now)
		} else {
			doc.Collection.Collectors[id] = carryForward(&doc, index, id, outcome.Err, prev, now)
		}
	}
	for i := range doc.Services {
		doc.Services[i].Evidence.IPRanges = dedupeRanges(doc.Services[i].Evidence.IPRanges)
	}
	model.Normalize(&doc)
	hash, err := model.ContentHash(doc)
	if err != nil {
		return model.Document{}, err
	}
	doc.Collection.ContentHash = hash
	return doc, nil
}

func curated(url string) model.Provenance {
	return model.Provenance{Source: model.SourceCurated, SourceURL: url}
}

func curatedService(s definitions.ServiceDef) model.Service {
	out := model.Service{Key: s.Key, DisplayName: s.DisplayName, ServiceTypes: s.ServiceTypes, Traits: s.Traits}
	e := &out.Evidence
	for _, r := range s.IPRanges {
		e.IPRanges = append(e.IPRanges, model.IPRange{CIDR: r.CIDR, Region: r.Region,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Note: r.Note, Provenance: curated(r.SourceURL)})
	}
	for _, a := range s.ASNs {
		e.ASNs = append(e.ASNs, model.ASN{ASN: a.ASN,
			Confidence: definitions.ConfidenceOr(a.Confidence, curatedConfidence), Note: a.Note, Provenance: curated(a.SourceURL)})
	}
	for _, r := range s.DNSRules {
		e.DNSRules = append(e.DNSRules, model.DNSRule{RecordType: r.RecordType, MatchField: r.MatchField,
			MatcherType: r.MatcherType, Pattern: r.Pattern, CaseSensitive: r.CaseSensitive,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Priority: r.Priority,
			Note: r.Note, Provenance: curated(r.SourceURL)})
	}
	for _, r := range s.HTTPRules {
		e.HTTPRules = append(e.HTTPRules, model.HTTPRule{HTTPPart: r.HTTPPart, HeaderName: r.HeaderName,
			MatcherType: r.MatcherType, Pattern: r.Pattern, PathScope: r.PathScope, CaseSensitive: r.CaseSensitive,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Priority: r.Priority,
			Note: r.Note, Provenance: curated(r.SourceURL)})
	}
	for _, r := range s.PTRRules {
		e.PTRRules = append(e.PTRRules, model.PTRRule{MatcherType: r.MatcherType, Pattern: r.Pattern,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Note: r.Note, Provenance: curated(r.SourceURL)})
	}
	for _, c := range s.CertificateIdentities {
		e.CertificateIdentities = append(e.CertificateIdentities, model.CertificateIdentity{IdentityType: c.IdentityType,
			IdentityValue: c.IdentityValue, Confidence: definitions.ConfidenceOr(c.Confidence, curatedConfidence),
			Note: c.Note, Provenance: curated(c.SourceURL)})
	}
	return out
}

func applyFeed(doc *model.Document, index map[string]int, ref definitions.FeedRef, res feeds.Result, now time.Time) model.CollectorStatus {
	id := ref.ID()
	source, confidence := model.SourceOfficialFeed, officialConfidence
	if bgpCollectors[ref.Collector] {
		source, confidence = model.SourceBGP, bgpConfidence
	}
	specific := map[netip.Prefix]bool{}
	for _, r := range res.Ranges {
		if !ref.IsGeneric(r.Tag) {
			specific[r.Prefix] = true
		}
	}
	unmapped := map[string]bool{}
	items := 0
	for _, r := range res.Ranges {
		if ref.IsGeneric(r.Tag) && specific[r.Prefix] {
			continue
		}
		svcKey, ok := ref.ResolveTag(r.Tag)
		if !ok {
			unmapped[r.Tag] = true
			continue
		}
		if svcKey == "" {
			continue
		}
		i, ok := index[svcKey]
		if !ok {
			unmapped[r.Tag] = true
			continue
		}
		doc.Services[i].Evidence.IPRanges = append(doc.Services[i].Evidence.IPRanges, model.IPRange{
			CIDR: r.Prefix.String(), Region: r.Region, FeedTag: r.Tag, Confidence: confidence,
			Provenance: model.Provenance{Source: source, Collector: id, SourceURL: res.SourceURL, SourceVersion: res.SourceVersion},
		})
		items++
	}
	success := now
	return model.CollectorStatus{
		Status: "ok", SourceURL: res.SourceURL, SourceVersion: res.SourceVersion, Items: items,
		FetchedAt: now, LastSuccessAt: &success, SkippedLines: res.Skipped, UnmappedTags: sortedKeys(unmapped),
	}
}

func carryForward(doc *model.Document, index map[string]int, id string, cause error, prev *model.Document, now time.Time) model.CollectorStatus {
	status := model.CollectorStatus{Status: "failed", FetchedAt: now, Error: cause.Error()}
	if prev == nil {
		return status
	}
	prevStatus, ok := prev.Collection.Collectors[id]
	if !ok || prevStatus.LastSuccessAt == nil {
		return status
	}
	items := 0
	for _, svc := range prev.Services {
		i, ok := index[svc.Key]
		if !ok {
			continue
		}
		for _, r := range svc.Evidence.IPRanges {
			if r.Collector != id {
				continue
			}
			doc.Services[i].Evidence.IPRanges = append(doc.Services[i].Evidence.IPRanges, r)
			items++
		}
	}
	status.Status = "stale"
	status.SourceURL = prevStatus.SourceURL
	status.SourceVersion = prevStatus.SourceVersion
	status.Items = items
	status.LastSuccessAt = prevStatus.LastSuccessAt
	return status
}

var sourceRank = map[model.Source]int{model.SourceOfficialFeed: 0, model.SourceCurated: 1, model.SourceBGP: 2}

// dedupeRanges keeps one item per CIDR: the one with a region, then the most
// authoritative source, then a stable tie-break on collector and tag.
func dedupeRanges(in []model.IPRange) []model.IPRange {
	groups := map[string][]model.IPRange{}
	for _, r := range in {
		groups[r.CIDR] = append(groups[r.CIDR], r)
	}
	out := make([]model.IPRange, 0, len(groups))
	for _, g := range groups {
		sort.SliceStable(g, func(i, j int) bool {
			a, b := g[i], g[j]
			if (a.Region == "") != (b.Region == "") {
				return a.Region != ""
			}
			if sourceRank[a.Source] != sourceRank[b.Source] {
				return sourceRank[a.Source] < sourceRank[b.Source]
			}
			if a.Collector != b.Collector {
				return a.Collector < b.Collector
			}
			if a.FeedTag != b.FeedTag {
				return a.FeedTag < b.FeedTag
			}
			return a.Region < b.Region
		})
		out = append(out, g[0])
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
