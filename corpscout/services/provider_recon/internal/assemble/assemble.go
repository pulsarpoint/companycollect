// Package assemble turns a curated definition plus this run's feed outcomes
// into a provider-recon/v1 document, reconciling every item's lifecycle
// against the previous document.
package assemble

import (
	"encoding/json"
	"errors"
	"fmt"
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
	// Format is the collector's Format(), for operators.
	Format string
}

const (
	curatedConfidence  = 1.0
	officialConfidence = 1.0
	bgpConfidence      = 0.8
	// RemovedRetentionDays is how long a removed item stays in latest.json.
	// History objects and the ClickHouse timeline keep it forever.
	RemovedRetentionDays = 90
)

// bgpCollectors publish announcements, not operator-published ranges.
var bgpCollectors = map[string]bool{"ripestat_announced": true}

// Restore failures, wrapped so callers can map them (the HTTP service does).
var (
	ErrUnknownCollector = errors.New("unknown collector")
	ErrBadDate          = errors.New("invalid date")
	ErrNothingToRestore = errors.New("nothing to restore")
)

// svcRange is a feed range with the service it belongs to.
type svcRange struct {
	svc  string
	item model.IPRange
}

// Build assembles the document for one run.
//
// Curated evidence follows the definition: an item deleted there is removed
// at once. Feed ranges follow the feed: a range absent from a successful
// fetch goes missing, and is removed once its grace period has passed. A
// failed fetch changes nothing (stale). No threshold blocks a large drop; it
// shows up in the collector's churn, and Restore undoes a wrong removal.
func Build(def definitions.Definition, outcomes map[string]FeedOutcome, prev *model.Document, now time.Time) (model.Document, error) {
	today := now.UTC().Format(model.DateLayout)
	prev = upgrade(prev)
	doc := model.Document{
		Version: model.ContractVersion, Slug: def.Slug, DisplayName: def.DisplayName,
		Category: def.Category, Website: def.Website, Country: def.Country,
		Aliases: def.Aliases, ProviderKeys: def.ProviderKeys,
		Collection: model.Collection{CollectedAt: now, Collectors: map[string]model.CollectorStatus{}},
	}

	index := map[string]int{}
	defined := map[string]bool{}
	curatedBy := map[string]model.Evidence{}
	for _, s := range def.Services {
		defined[s.Key] = true
		curatedBy[s.Key] = curatedEvidence(s)
		index[s.Key] = len(doc.Services)
		doc.Services = append(doc.Services, model.Service{Key: s.Key, DisplayName: s.DisplayName, ServiceTypes: s.ServiceTypes, Traits: s.Traits})
	}
	prevEvidence := map[string]model.Evidence{}
	if prev != nil {
		for _, ps := range prev.Services {
			prevEvidence[ps.Key] = ps.Evidence
			if defined[ps.Key] {
				continue
			}
			// Dropped from the definition: kept until its removed items are purged.
			removedAt := ps.RemovedAt
			if removedAt == "" {
				removedAt = today
			}
			index[ps.Key] = len(doc.Services)
			doc.Services = append(doc.Services, model.Service{Key: ps.Key, DisplayName: ps.DisplayName,
				ServiceTypes: ps.ServiceTypes, Traits: ps.Traits, RemovedAt: removedAt})
		}
	}
	for i := range doc.Services {
		svc := &doc.Services[i]
		svc.Evidence = reconcileCurated(prevEvidence[svc.Key], curatedBy[svc.Key], today)
	}

	feedIDs := map[string]bool{}
	for _, ref := range def.Feeds {
		id := ref.ID()
		feedIDs[id] = true
		var prevStatus model.CollectorStatus
		if prev != nil {
			prevStatus = prev.Collection.Collectors[id]
		}
		prevItems := feedItems(prev, func(collector string) bool { return collector == id })
		outcome, ran := outcomes[id]
		if !ran {
			outcome = FeedOutcome{Err: errors.New("collector did not run")}
		}
		var status model.CollectorStatus
		if outcome.Err == nil {
			status = applyFeed(&doc, index, defined, ref, outcome.Result, prevItems, today, now)
		} else {
			status = carryForward(&doc, index, prevItems, prevStatus, outcome.Err, now)
		}
		status.Format = outcome.Format
		if status.Format == "" {
			status.Format = prevStatus.Format
		}
		doc.Collection.Collectors[id] = status
	}
	// Feeds dropped from the definition: their ranges are removed now.
	for _, p := range feedItems(prev, func(collector string) bool { return !feedIDs[collector] }) {
		retire(&p.item.Lifecycle, model.ActionDefinitionRemoved, today)
		appendRange(&doc, index, p)
	}

	purge(&doc, today)
	model.Normalize(&doc)
	hash, err := model.ContentHash(doc)
	if err != nil {
		return model.Document{}, err
	}
	doc.Collection.ContentHash = hash
	return doc, nil
}

// Restore undoes wrong removals: every range of one feed that grace expiry
// removed on or after removedSince (YYYY-MM-DD) goes back to active, with
// RestoredAt recording the correction. Definition removals are never
// restored, and neither is an instance whose range already came back as a new
// live instance. It returns how many ranges it restored.
func Restore(doc model.Document, collectorID, removedSince string, now time.Time) (model.Document, int, error) {
	st, ok := doc.Collection.Collectors[collectorID]
	if !ok {
		return doc, 0, fmt.Errorf("%s has no collector %q: %w", doc.Slug, collectorID, ErrUnknownCollector)
	}
	if _, err := time.Parse(model.DateLayout, removedSince); err != nil {
		return doc, 0, fmt.Errorf("removed-since %q is not a YYYY-MM-DD date: %w", removedSince, ErrBadDate)
	}
	cp, err := clone(doc)
	if err != nil {
		return doc, 0, err
	}
	today := now.UTC().Format(model.DateLayout)
	n := 0
	for i := range cp.Services {
		rs := cp.Services[i].Evidence.IPRanges
		// Only the newest instance of a CIDR can come back: reviving an older
		// one would merge validity intervals across a real gap, and a live
		// newest instance means the range is already back.
		newest := map[string]int{}
		for j, r := range rs {
			if r.Collector != collectorID {
				continue
			}
			if k, seen := newest[r.CIDR]; !seen || newerInstance(r, rs[k]) {
				newest[r.CIDR] = j
			}
		}
		for _, j := range newest {
			r := &rs[j]
			if r.Status != model.StatusRemoved || r.RemovalAction != model.ActionGraceExpired || r.RemovedAt < removedSince {
				continue
			}
			r.Status, r.RemovedAt, r.RemovalAction, r.MissingSince, r.RestoredAt = model.StatusActive, "", "", "", today
			n++
		}
	}
	if n == 0 {
		return doc, 0, fmt.Errorf("%s %s has no grace-expired removals on or after %s: %w", doc.Slug, collectorID, removedSince, ErrNothingToRestore)
	}
	// A restore run is not a collection: no feed added, lost or removed
	// anything, so every collector's churn is zero except the restored count.
	for id, cs := range cp.Collection.Collectors {
		cs.Churn = model.Churn{}
		cp.Collection.Collectors[id] = cs
	}
	st = cp.Collection.Collectors[collectorID]
	st.Items = countLive(&cp, collectorID)
	st.Churn = model.Churn{Restored: n}
	cp.Collection.Collectors[collectorID] = st
	model.Normalize(&cp)
	hash, err := model.ContentHash(cp)
	if err != nil {
		return doc, 0, err
	}
	cp.Collection.ContentHash = hash
	return cp, n, nil
}

// newerInstance orders instances of one CIDR: later first_seen wins, then a
// live instance over a removed one, then the later removal.
func newerInstance(a, b model.IPRange) bool {
	if a.FirstSeen != b.FirstSeen {
		return a.FirstSeen > b.FirstSeen
	}
	if (a.Status == model.StatusRemoved) != (b.Status == model.StatusRemoved) {
		return a.Status != model.StatusRemoved
	}
	return a.RemovedAt > b.RemovedAt
}

func applyFeed(doc *model.Document, index map[string]int, defined map[string]bool, ref definitions.FeedRef,
	res feeds.Result, prevItems []svcRange, today string, now time.Time) model.CollectorStatus {
	id := ref.ID()
	source, confidence := model.SourceOfficialFeed, officialConfidence
	if bgpCollectors[ref.Collector] {
		source, confidence = model.SourceBGP, bgpConfidence
	}

	// Only a tag that resolves to one of this provider's services claims a
	// prefix away from the umbrella tag. Ignored ("") and unmapped tags do not,
	// or the prefix would vanish from the provider entirely.
	specific := map[netip.Prefix]bool{}
	for _, r := range res.Ranges {
		if ref.IsGeneric(r.Tag) {
			continue
		}
		if svcKey, ok := ref.ResolveTag(r.Tag); ok && svcKey != "" && defined[svcKey] {
			specific[r.Prefix] = true
		}
	}

	// This run's observations, one per (service, CIDR) within this feed.
	unmapped := map[string]bool{}
	observed := map[string]svcRange{}
	for _, r := range res.Ranges {
		if ref.IsGeneric(r.Tag) && specific[r.Prefix] {
			continue
		}
		svcKey, ok := ref.ResolveTag(r.Tag)
		if !ok || (svcKey != "" && !defined[svcKey]) {
			unmapped[r.Tag] = true
			continue
		}
		if svcKey == "" {
			continue
		}
		item := model.IPRange{CIDR: r.Prefix.String(), Region: r.Region, FeedTag: r.Tag, Confidence: confidence,
			Provenance: model.Provenance{Source: source, Collector: id, SourceURL: res.SourceURL, SourceVersion: res.SourceVersion}}
		k := svcKey + "|" + item.CIDR
		if cur, dup := observed[k]; dup && !better(item, cur.item) {
			continue
		}
		observed[k] = svcRange{svc: svcKey, item: item}
	}

	live := map[string]svcRange{}
	var liveKeys []string
	// Removed instances first seen today are revived by a same-day return
	// (see reconcile): one (service, CIDR, collector, first_seen) per range.
	sameDay := map[string]svcRange{}
	var sameDayKeys []string
	for _, p := range prevItems {
		if p.item.Status == model.StatusRemoved {
			if k := p.svc + "|" + p.item.CIDR; p.item.FirstSeen == today {
				if _, dup := sameDay[k]; !dup {
					sameDay[k] = p
					sameDayKeys = append(sameDayKeys, k)
					continue
				}
			}
			appendRange(doc, index, p)
			continue
		}
		k := p.svc + "|" + p.item.CIDR
		if _, dup := live[k]; !dup {
			liveKeys = append(liveKeys, k)
		}
		live[k] = p
	}

	var churn model.Churn
	for _, k := range sortedMapKeys(observed) {
		o := observed[k]
		if p, ok := live[k]; ok {
			o.item.Lifecycle = p.item.Lifecycle
			if p.item.Status == model.StatusMissing {
				churn.Reappeared++
			}
			delete(live, k)
		} else if p, ok := sameDay[k]; ok {
			o.item.Lifecycle = p.item.Lifecycle
			o.item.RemovedAt, o.item.RemovalAction = "", ""
			churn.Reappeared++
			delete(sameDay, k)
		} else {
			o.item.Lifecycle = model.Lifecycle{FirstSeen: today}
			churn.Added++
		}
		o.item.Status, o.item.LastSeen, o.item.MissingSince = model.StatusActive, today, ""
		appendRange(doc, index, o)
	}
	for _, k := range sameDayKeys {
		if p, ok := sameDay[k]; ok {
			appendRange(doc, index, p)
		}
	}

	var missing []svcRange
	sort.Strings(liveKeys)
	for _, k := range liveKeys {
		p, ok := live[k]
		if !ok {
			continue
		}
		if !defined[p.svc] {
			retire(&p.item.Lifecycle, model.ActionDefinitionRemoved, today)
			churn.Removed++
			appendRange(doc, index, p)
			continue
		}
		if p.item.Status == model.StatusActive {
			p.item.Status, p.item.MissingSince = model.StatusMissing, today
			churn.Missing++
		}
		missing = append(missing, p)
	}
	for _, p := range missing {
		if daysBetween(p.item.MissingSince, today) >= ref.Grace() {
			retire(&p.item.Lifecycle, model.ActionGraceExpired, today)
			churn.Removed++
		}
		appendRange(doc, index, p)
	}

	success := now
	return model.CollectorStatus{
		Status: "ok", SourceURL: res.SourceURL, SourceVersion: res.SourceVersion,
		Items: countLive(doc, id), FetchedAt: now, LastSuccessAt: &success, SkippedLines: res.Skipped,
		UnmappedTags: sortedKeys(unmapped), Churn: churn,
	}
}

// carryForward keeps a failed feed's ranges exactly as they were: nothing can
// be observed missing, and nothing expires, while the feed is down.
func carryForward(doc *model.Document, index map[string]int, prevItems []svcRange, prevStatus model.CollectorStatus, cause error, now time.Time) model.CollectorStatus {
	items := 0
	for _, p := range prevItems {
		appendRange(doc, index, p)
		if p.item.Status != model.StatusRemoved {
			items++
		}
	}
	status := model.CollectorStatus{Status: "failed", FetchedAt: now, Error: cause.Error()}
	if prevStatus.LastSuccessAt == nil {
		return status
	}
	status.Status = "stale"
	status.SourceURL = prevStatus.SourceURL
	status.SourceVersion = prevStatus.SourceVersion
	status.Items = items
	status.LastSuccessAt = prevStatus.LastSuccessAt
	return status
}

func curatedProvenance(url string) model.Provenance {
	return model.Provenance{Source: model.SourceCurated, SourceURL: url}
}

func curatedEvidence(s definitions.ServiceDef) model.Evidence {
	var e model.Evidence
	for _, r := range s.IPRanges {
		e.IPRanges = append(e.IPRanges, model.IPRange{CIDR: r.CIDR, Region: r.Region,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Note: r.Note, Provenance: curatedProvenance(r.SourceURL)})
	}
	for _, a := range s.ASNs {
		e.ASNs = append(e.ASNs, model.ASN{ASN: a.ASN,
			Confidence: definitions.ConfidenceOr(a.Confidence, curatedConfidence), Note: a.Note, Provenance: curatedProvenance(a.SourceURL)})
	}
	for _, r := range s.DNSRules {
		e.DNSRules = append(e.DNSRules, model.DNSRule{RecordType: r.RecordType, MatchField: r.MatchField,
			MatcherType: r.MatcherType, Pattern: r.Pattern, CaseSensitive: r.CaseSensitive,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Priority: r.Priority,
			Note: r.Note, Provenance: curatedProvenance(r.SourceURL)})
	}
	for _, r := range s.HTTPRules {
		e.HTTPRules = append(e.HTTPRules, model.HTTPRule{HTTPPart: r.HTTPPart, HeaderName: r.HeaderName,
			MatcherType: r.MatcherType, Pattern: r.Pattern, PathScope: r.PathScope, CaseSensitive: r.CaseSensitive,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Priority: r.Priority,
			Note: r.Note, Provenance: curatedProvenance(r.SourceURL)})
	}
	for _, r := range s.PTRRules {
		e.PTRRules = append(e.PTRRules, model.PTRRule{MatcherType: r.MatcherType, Pattern: r.Pattern,
			Confidence: definitions.ConfidenceOr(r.Confidence, curatedConfidence), Note: r.Note, Provenance: curatedProvenance(r.SourceURL)})
	}
	for _, c := range s.CertificateIdentities {
		e.CertificateIdentities = append(e.CertificateIdentities, model.CertificateIdentity{IdentityType: c.IdentityType,
			IdentityValue: c.IdentityValue, Confidence: definitions.ConfidenceOr(c.Confidence, curatedConfidence),
			Note: c.Note, Provenance: curatedProvenance(c.SourceURL)})
	}
	return e
}

// reconcileCurated carries the lifecycle of curated items that are still
// defined, starts new ones, and removes the ones the definition dropped. Feed
// ranges are handled per feed, not here.
func reconcileCurated(prev, cur model.Evidence, today string) model.Evidence {
	return model.Evidence{
		IPRanges: reconcile(prev.IPRanges, cur.IPRanges, model.IPRange.Key,
			func(x *model.IPRange) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance }, today),
		ASNs: reconcile(prev.ASNs, cur.ASNs, model.ASN.Key,
			func(x *model.ASN) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance }, today),
		DNSRules: reconcile(prev.DNSRules, cur.DNSRules, model.DNSRule.Key,
			func(x *model.DNSRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance }, today),
		HTTPRules: reconcile(prev.HTTPRules, cur.HTTPRules, model.HTTPRule.Key,
			func(x *model.HTTPRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance }, today),
		PTRRules: reconcile(prev.PTRRules, cur.PTRRules, model.PTRRule.Key,
			func(x *model.PTRRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance }, today),
		CertificateIdentities: reconcile(prev.CertificateIdentities, cur.CertificateIdentities, model.CertificateIdentity.Key,
			func(x *model.CertificateIdentity) (*model.Lifecycle, *model.Provenance) {
				return &x.Lifecycle, &x.Provenance
			}, today),
	}
}

func reconcile[T any](prev, cur []T, key func(T) string, parts func(*T) (*model.Lifecycle, *model.Provenance), today string) []T {
	out := make([]T, 0, len(cur))
	live := map[string]T{}
	var liveKeys []string
	// A removed instance first seen today is revived by a same-day re-add:
	// two instances with one (key, first_seen) would be indistinguishable
	// downstream (ClickHouse keys on it) and the removed twin would win.
	sameDay := map[string]T{}
	var sameDayKeys []string
	for _, p := range prev {
		l, pv := parts(&p)
		if pv.Source != model.SourceCurated {
			continue
		}
		if l.Status == model.StatusRemoved {
			if k := key(p); l.FirstSeen == today {
				if _, dup := sameDay[k]; !dup {
					sameDay[k] = p
					sameDayKeys = append(sameDayKeys, k)
					continue
				}
			}
			out = append(out, p)
			continue
		}
		k := key(p)
		if _, dup := live[k]; !dup {
			liveKeys = append(liveKeys, k)
		}
		live[k] = p
	}
	for _, c := range cur {
		k := key(c)
		l, _ := parts(&c)
		if p, ok := live[k]; ok {
			pl, _ := parts(&p)
			*l = *pl
			delete(live, k)
		} else if p, ok := sameDay[k]; ok {
			pl, _ := parts(&p)
			*l = *pl
			l.RemovedAt, l.RemovalAction = "", ""
			delete(sameDay, k)
		} else {
			*l = model.Lifecycle{FirstSeen: today}
		}
		l.Status, l.LastSeen, l.MissingSince = model.StatusActive, today, ""
		out = append(out, c)
	}
	for _, k := range sameDayKeys {
		if p, ok := sameDay[k]; ok {
			out = append(out, p)
		}
	}
	for _, k := range liveKeys {
		p, ok := live[k]
		if !ok {
			continue
		}
		l, _ := parts(&p)
		retire(l, model.ActionDefinitionRemoved, today)
		out = append(out, p)
	}
	return out
}

func retire(l *model.Lifecycle, action, today string) {
	if l.Status == model.StatusRemoved {
		return
	}
	l.Status, l.RemovedAt, l.RemovalAction = model.StatusRemoved, today, action
}

// feedItems returns the previous document's feed ranges whose collector
// matches.
func feedItems(prev *model.Document, match func(collector string) bool) []svcRange {
	if prev == nil {
		return nil
	}
	var out []svcRange
	for _, s := range prev.Services {
		for _, r := range s.Evidence.IPRanges {
			if r.Source != model.SourceCurated && r.Collector != "" && match(r.Collector) {
				out = append(out, svcRange{svc: s.Key, item: r})
			}
		}
	}
	return out
}

func appendRange(doc *model.Document, index map[string]int, p svcRange) {
	i, ok := index[p.svc]
	if !ok {
		index[p.svc] = len(doc.Services)
		doc.Services = append(doc.Services, model.Service{Key: p.svc, RemovedAt: p.item.RemovedAt})
		i = index[p.svc]
	}
	doc.Services[i].Evidence.IPRanges = append(doc.Services[i].Evidence.IPRanges, p.item)
}

func countLive(doc *model.Document, collector string) int {
	n := 0
	for _, s := range doc.Services {
		for _, r := range s.Evidence.IPRanges {
			if r.Collector == collector && r.Status != model.StatusRemoved {
				n++
			}
		}
	}
	return n
}

// better prefers a range with a region, then a stable tag/region order.
func better(a, b model.IPRange) bool {
	if (a.Region == "") != (b.Region == "") {
		return a.Region != ""
	}
	if a.FeedTag != b.FeedTag {
		return a.FeedTag < b.FeedTag
	}
	return a.Region < b.Region
}

// purge drops removed items older than the retention period, and services
// dropped from the definition once they hold no items.
func purge(doc *model.Document, today string) {
	out := doc.Services[:0:0]
	for _, s := range doc.Services {
		e := &s.Evidence
		e.IPRanges = keepFresh(e.IPRanges, func(x *model.IPRange) *model.Lifecycle { return &x.Lifecycle }, today, func(x model.IPRange) {
			if st, ok := doc.Collection.Collectors[x.Collector]; ok {
				st.Churn.Purged++
				doc.Collection.Collectors[x.Collector] = st
			}
		})
		e.ASNs = keepFresh(e.ASNs, func(x *model.ASN) *model.Lifecycle { return &x.Lifecycle }, today, nil)
		e.DNSRules = keepFresh(e.DNSRules, func(x *model.DNSRule) *model.Lifecycle { return &x.Lifecycle }, today, nil)
		e.HTTPRules = keepFresh(e.HTTPRules, func(x *model.HTTPRule) *model.Lifecycle { return &x.Lifecycle }, today, nil)
		e.PTRRules = keepFresh(e.PTRRules, func(x *model.PTRRule) *model.Lifecycle { return &x.Lifecycle }, today, nil)
		e.CertificateIdentities = keepFresh(e.CertificateIdentities, func(x *model.CertificateIdentity) *model.Lifecycle { return &x.Lifecycle }, today, nil)
		if s.RemovedAt != "" && len(e.IPRanges)+len(e.ASNs)+len(e.DNSRules)+len(e.HTTPRules)+len(e.PTRRules)+len(e.CertificateIdentities) == 0 {
			continue
		}
		out = append(out, s)
	}
	doc.Services = out
}

func keepFresh[T any](items []T, life func(*T) *model.Lifecycle, today string, onPurge func(T)) []T {
	out := items[:0:0]
	for _, it := range items {
		l := life(&it)
		if l.Status == model.StatusRemoved && daysBetween(l.RemovedAt, today) > RemovedRetentionDays {
			if onPurge != nil {
				onPurge(it)
			}
			continue
		}
		out = append(out, it)
	}
	return out
}

// upgrade gives items published before lifecycle tracking an active lifecycle
// starting on the day of that run. It works on a copy.
func upgrade(prev *model.Document) *model.Document {
	if prev == nil {
		return nil
	}
	cp, err := clone(*prev)
	if err != nil {
		return prev
	}
	since := cp.Collection.CollectedAt.UTC().Format(model.DateLayout)
	fix := func(l *model.Lifecycle) {
		if l.Status == "" {
			*l = model.Lifecycle{Status: model.StatusActive, FirstSeen: since, LastSeen: since}
		}
	}
	for i := range cp.Services {
		e := &cp.Services[i].Evidence
		for j := range e.IPRanges {
			fix(&e.IPRanges[j].Lifecycle)
		}
		for j := range e.ASNs {
			fix(&e.ASNs[j].Lifecycle)
		}
		for j := range e.DNSRules {
			fix(&e.DNSRules[j].Lifecycle)
		}
		for j := range e.HTTPRules {
			fix(&e.HTTPRules[j].Lifecycle)
		}
		for j := range e.PTRRules {
			fix(&e.PTRRules[j].Lifecycle)
		}
		for j := range e.CertificateIdentities {
			fix(&e.CertificateIdentities[j].Lifecycle)
		}
	}
	return &cp
}

func clone(doc model.Document) (model.Document, error) {
	raw, err := json.Marshal(doc)
	if err != nil {
		return model.Document{}, err
	}
	var cp model.Document
	err = json.Unmarshal(raw, &cp)
	return cp, err
}

func daysBetween(from, to string) int {
	a, err1 := time.Parse(model.DateLayout, from)
	b, err2 := time.Parse(model.DateLayout, to)
	if err1 != nil || err2 != nil {
		return 0
	}
	return int(b.Sub(a).Hours() / 24)
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

func sortedMapKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
