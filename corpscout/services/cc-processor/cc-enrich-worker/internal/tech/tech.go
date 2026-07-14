package tech

import (
	"sort"

	"cc-enrich-worker/internal/model"

	wappalyzer "github.com/projectdiscovery/wappalyzergo"
)

var wapp, _ = wappalyzer.New()

// fastTech, when set (via --tech-engine fast), replaces wappalyzergo's full scan with
// the Aho-Corasick-gated matcher. nil => upstream wappalyzergo.
var fastTech *FastMatcher

// SetFastMatcher wires in the optional Aho-Corasick-gated matcher. Call from main
// when --tech-engine=fast; nil resets to upstream wappalyzergo.
func SetFastMatcher(fm *FastMatcher) { fastTech = fm }

// DetectTech fingerprints one page (HTTP headers + body) into a list of technologies.
// wappalyzergo returns keys as "Name" or "Name:version"; categories come from AppInfo.
func DetectTech(headers map[string][]string, body []byte) []model.Technology {
	if fastTech != nil {
		return fastTech.Detect(headers, body)
	}
	out := []model.Technology{}
	if wapp == nil {
		return out
	}
	for key, app := range wapp.FingerprintWithInfo(headers, body) {
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
