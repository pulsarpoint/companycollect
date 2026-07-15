package tech

import (
	"sort"

	"cc-enrich-worker/internal/model"

	wappalyzer "github.com/projectdiscovery/wappalyzergo"
)

// Detector fingerprints one page (HTTP headers + body) into a list of technologies. The engine is
// an explicit dependency: buildPartDeps constructs one per --tech-engine and threads it to the page
// loop via ShardConfig — there is no package-level default.
//
// Implementations: *FastMatcher (--tech-engine fast, Aho-Corasick gated) and *Wappalyzer
// (--tech-engine wappalyzer, upstream full scan). Both are immutable after construction and safe
// for concurrent Detect calls.
type Detector interface {
	Detect(headers map[string][]string, body []byte) []model.Technology
}

var (
	_ Detector = (*FastMatcher)(nil)
	_ Detector = (*Wappalyzer)(nil)
)

// Wappalyzer is the upstream wappalyzergo full-scan engine.
type Wappalyzer struct {
	w *wappalyzer.Wappalyze
}

// NewWappalyzer builds the upstream engine from wappalyzergo's embedded fingerprints.
func NewWappalyzer() (*Wappalyzer, error) {
	w, err := wappalyzer.New()
	if err != nil {
		return nil, err
	}
	return &Wappalyzer{w: w}, nil
}

// Detect fingerprints one page with the upstream full scan.
// wappalyzergo returns keys as "Name" or "Name:version"; categories come from AppInfo.
func (wz *Wappalyzer) Detect(headers map[string][]string, body []byte) []model.Technology {
	out := []model.Technology{}
	for key, app := range wz.w.FingerprintWithInfo(headers, body) {
		name, version := key, ""
		for i := 0; i < len(key); i++ {
			if key[i] == ':' {
				name, version = key[:i], key[i+1:]
				break
			}
		}
		cat := ""
		if len(app.Categories) > 0 {
			cat = app.Categories[0]
		}
		out = append(out, model.Technology{Name: name, Category: cat, Version: version, Confidence: 100})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Version < out[j].Version
	})
	return out
}
