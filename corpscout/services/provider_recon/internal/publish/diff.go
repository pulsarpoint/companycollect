package publish

import (
	"encoding/json"
	"sort"

	"provider_recon/internal/model"
)

// maxListed caps the item lists in a change; counts stay exact.
const maxListed = 1000

// KindDiff is one evidence kind's lifecycle transitions in a run. Items are
// labelled "<service> <key>"; removals carry the action in parentheses;
// restored means restore undid a removal; purged means the item left the
// document (retention, or an alias/key edit).
type KindDiff struct {
	Added           []string `json:"added,omitempty"`
	Missing         []string `json:"missing,omitempty"`
	Reappeared      []string `json:"reappeared,omitempty"`
	Restored        []string `json:"restored,omitempty"`
	Removed         []string `json:"removed,omitempty"`
	Purged          []string `json:"purged,omitempty"`
	Updated         []string `json:"updated,omitempty"`
	AddedCount      int      `json:"added_count"`
	MissingCount    int      `json:"missing_count"`
	ReappearedCount int      `json:"reappeared_count"`
	RestoredCount   int      `json:"restored_count"`
	RemovedCount    int      `json:"removed_count"`
	PurgedCount     int      `json:"purged_count"`
	UpdatedCount    int      `json:"updated_count"`
	Truncated       bool     `json:"truncated,omitempty"`
}

func (d KindDiff) empty() bool {
	return d.AddedCount+d.MissingCount+d.ReappearedCount+d.RestoredCount+d.RemovedCount+d.PurgedCount+d.UpdatedCount == 0
}

// VersionChange is a feed version before and after the run.
type VersionChange struct {
	Old string `json:"old"`
	New string `json:"new"`
}

// ProviderChange describes what changed for one provider in a run.
type ProviderChange struct {
	Slug         string                   `json:"slug"`
	Created      bool                     `json:"created,omitempty"`
	OldHash      string                   `json:"old_hash,omitempty"`
	NewHash      string                   `json:"new_hash"`
	Evidence     map[string]KindDiff      `json:"evidence,omitempty"`
	FeedVersions map[string]VersionChange `json:"feed_versions,omitempty"`
}

var kinds = []string{"aliases", "asns", "certificate_identities", "dns_rules", "http_rules", "ip_ranges", "provider_keys", "ptr_rules", "services"}

// entry is one item instance; identity = label + first_seen, so a removed
// instance and its re-added successor are distinct.
type entry struct {
	label, status, action, canon string
}

type itemSet map[string]entry

// Diff compares cur with the previous document (nil for a new provider). A
// new provider reports counts only.
func Diff(old *model.Document, cur model.Document) ProviderChange {
	ch := ProviderChange{Slug: cur.Slug, NewHash: cur.Collection.ContentHash, Created: old == nil,
		Evidence: map[string]KindDiff{}, FeedVersions: map[string]VersionChange{}}
	if old != nil {
		ch.OldHash = old.Collection.ContentHash
	}
	before, after := itemSets(old), itemSets(&cur)
	for _, kind := range kinds {
		if d := diffKind(before[kind], after[kind], !ch.Created); !d.empty() {
			ch.Evidence[kind] = d
		}
	}
	for id, st := range cur.Collection.Collectors {
		prev := ""
		if old != nil {
			prev = old.Collection.Collectors[id].SourceVersion
		}
		if prev != st.SourceVersion {
			ch.FeedVersions[id] = VersionChange{Old: prev, New: st.SourceVersion}
		}
	}
	return ch
}

func diffKind(before, after itemSet, list bool) KindDiff {
	var added, missing, reappeared, restored, removed, purged, updated []string
	for id, a := range after {
		b, ok := before[id]
		switch {
		case !ok && a.status == model.StatusRemoved:
			removed = append(removed, a.label+" ("+a.action+")")
		case !ok:
			added = append(added, a.label)
		case b.status != model.StatusMissing && a.status == model.StatusMissing:
			missing = append(missing, a.label)
		case b.status == model.StatusMissing && a.status == model.StatusActive:
			reappeared = append(reappeared, a.label)
		case b.status == model.StatusRemoved && a.status == model.StatusActive:
			restored = append(restored, a.label)
		case b.status != model.StatusRemoved && a.status == model.StatusRemoved:
			removed = append(removed, a.label+" ("+a.action+")")
		case b.canon != a.canon:
			updated = append(updated, a.label)
		}
	}
	for id, b := range before {
		if _, ok := after[id]; !ok {
			purged = append(purged, b.label)
		}
	}
	d := KindDiff{AddedCount: len(added), MissingCount: len(missing), ReappearedCount: len(reappeared),
		RestoredCount: len(restored), RemovedCount: len(removed), PurgedCount: len(purged), UpdatedCount: len(updated)}
	if !list {
		return d
	}
	var t [7]bool
	d.Added, t[0] = capList(added)
	d.Missing, t[1] = capList(missing)
	d.Reappeared, t[2] = capList(reappeared)
	d.Restored, t[3] = capList(restored)
	d.Removed, t[4] = capList(removed)
	d.Purged, t[5] = capList(purged)
	d.Updated, t[6] = capList(updated)
	d.Truncated = t[0] || t[1] || t[2] || t[3] || t[4] || t[5] || t[6]
	return d
}

func capList(xs []string) ([]string, bool) {
	sort.Strings(xs)
	if len(xs) > maxListed {
		return xs[:maxListed], true
	}
	return xs, false
}

func itemSets(doc *model.Document) map[string]itemSet {
	sets := map[string]itemSet{}
	for _, k := range kinds {
		sets[k] = itemSet{}
	}
	if doc == nil {
		return sets
	}
	for _, a := range doc.Aliases {
		sets["aliases"][a] = entry{label: a, status: model.StatusActive, canon: a}
	}
	for _, k := range doc.ProviderKeys {
		sets["provider_keys"][k] = entry{label: k, status: model.StatusActive, canon: k}
	}
	for _, s := range doc.Services {
		status := model.StatusActive
		if s.RemovedAt != "" {
			status = model.StatusRemoved
		}
		sets["services"][s.Key] = entry{label: s.Key, status: status, action: model.ActionDefinitionRemoved, canon: s.Key}
		e := s.Evidence
		addItems(sets["ip_ranges"], s.Key, e.IPRanges, model.IPRange.Key,
			func(x *model.IPRange) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance })
		addItems(sets["asns"], s.Key, e.ASNs, model.ASN.Key,
			func(x *model.ASN) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance })
		addItems(sets["dns_rules"], s.Key, e.DNSRules, model.DNSRule.Key,
			func(x *model.DNSRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance })
		addItems(sets["http_rules"], s.Key, e.HTTPRules, model.HTTPRule.Key,
			func(x *model.HTTPRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance })
		addItems(sets["ptr_rules"], s.Key, e.PTRRules, model.PTRRule.Key,
			func(x *model.PTRRule) (*model.Lifecycle, *model.Provenance) { return &x.Lifecycle, &x.Provenance })
		addItems(sets["certificate_identities"], s.Key, e.CertificateIdentities, model.CertificateIdentity.Key,
			func(x *model.CertificateIdentity) (*model.Lifecycle, *model.Provenance) {
				return &x.Lifecycle, &x.Provenance
			})
	}
	return sets
}

// addItems records each instance. canon is the item with the fields that move
// without the evidence changing blanked (last_seen, source version, a feed
// item's source URL), so "updated" means a real field change.
func addItems[T any](set itemSet, svc string, items []T, key func(T) string, parts func(*T) (*model.Lifecycle, *model.Provenance)) {
	for _, it := range items {
		l, p := parts(&it)
		label := svc + " " + key(it)
		e := entry{label: label, status: l.Status, action: l.RemovalAction}
		l.LastSeen = ""
		p.SourceVersion = ""
		if p.Source != model.SourceCurated {
			p.SourceURL = ""
		}
		canon, _ := json.Marshal(it)
		e.canon = string(canon)
		set[label+" @"+l.FirstSeen] = e
	}
}
