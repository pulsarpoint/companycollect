package publish

import (
	"sort"

	"provider_recon/internal/model"
)

// maxListed caps the item lists in a change; counts stay exact.
const maxListed = 1000

// KindDiff is the change of one evidence kind.
type KindDiff struct {
	Added        []string `json:"added,omitempty"`
	Removed      []string `json:"removed,omitempty"`
	AddedCount   int      `json:"added_count"`
	RemovedCount int      `json:"removed_count"`
	Truncated    bool     `json:"truncated,omitempty"`
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

// Diff compares cur with the previous document (nil for a new provider). A
// new provider reports counts only; listing every item of a first run is noise.
func Diff(old *model.Document, cur model.Document) ProviderChange {
	ch := ProviderChange{Slug: cur.Slug, NewHash: cur.Collection.ContentHash, Created: old == nil,
		Evidence: map[string]KindDiff{}, FeedVersions: map[string]VersionChange{}}
	if old != nil {
		ch.OldHash = old.Collection.ContentHash
	}
	before, after := itemSets(old), itemSets(&cur)
	for _, kind := range kinds {
		d := diffSets(before[kind], after[kind], !ch.Created)
		if d.AddedCount+d.RemovedCount > 0 {
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

func itemSets(doc *model.Document) map[string]map[string]bool {
	sets := map[string]map[string]bool{}
	for _, k := range kinds {
		sets[k] = map[string]bool{}
	}
	if doc == nil {
		return sets
	}
	for _, a := range doc.Aliases {
		sets["aliases"][a] = true
	}
	for _, k := range doc.ProviderKeys {
		sets["provider_keys"][k] = true
	}
	for _, s := range doc.Services {
		sets["services"][s.Key] = true
		e := s.Evidence
		for _, x := range e.IPRanges {
			sets["ip_ranges"][s.Key+" "+x.Key()] = true
		}
		for _, x := range e.ASNs {
			sets["asns"][s.Key+" "+x.Key()] = true
		}
		for _, x := range e.DNSRules {
			sets["dns_rules"][s.Key+" "+x.Key()] = true
		}
		for _, x := range e.HTTPRules {
			sets["http_rules"][s.Key+" "+x.Key()] = true
		}
		for _, x := range e.PTRRules {
			sets["ptr_rules"][s.Key+" "+x.Key()] = true
		}
		for _, x := range e.CertificateIdentities {
			sets["certificate_identities"][s.Key+" "+x.Key()] = true
		}
	}
	return sets
}

func diffSets(before, after map[string]bool, list bool) KindDiff {
	var d KindDiff
	var added, removed []string
	for k := range after {
		if !before[k] {
			added = append(added, k)
		}
	}
	for k := range before {
		if !after[k] {
			removed = append(removed, k)
		}
	}
	d.AddedCount, d.RemovedCount = len(added), len(removed)
	if !list {
		return d
	}
	sort.Strings(added)
	sort.Strings(removed)
	if len(added) > maxListed {
		added, d.Truncated = added[:maxListed], true
	}
	if len(removed) > maxListed {
		removed, d.Truncated = removed[:maxListed], true
	}
	d.Added, d.Removed = added, removed
	return d
}
