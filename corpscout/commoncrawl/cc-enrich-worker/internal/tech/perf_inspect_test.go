package tech

import (
	"encoding/json"
	"testing"

	wappalyzer "github.com/projectdiscovery/wappalyzergo"
)

func TestInspectMatcherShape(t *testing.T) {
	matcher, err := NewFastMatcher()
	if err != nil {
		t.Fatal(err)
	}
	htmlGated := 0
	for _, patterns := range matcher.litToPats {
		htmlGated += len(patterns)
	}
	scriptGated := 0
	for _, patterns := range matcher.litToScriptPats {
		scriptGated += len(patterns)
	}
	headerPatterns := 0
	for _, patterns := range matcher.headerPats {
		headerPatterns += len(patterns)
	}
	metaPatterns := 0
	for _, patterns := range matcher.metaPats {
		metaPatterns += len(patterns)
	}
	cookiePatterns := 0
	for _, patterns := range matcher.cookiePats {
		cookiePatterns += len(patterns)
	}
	t.Logf("html gated=%d literals=%d always=%d; script gated=%d literals=%d always=%d; headers=%d meta=%d cookies=%d",
		htmlGated, len(matcher.litToPats), len(matcher.alwaysHTML),
		scriptGated, len(matcher.litToScriptPats), len(matcher.alwaysScript),
		headerPatterns, metaPatterns, cookiePatterns)

	var raw struct {
		Apps map[string]rawApp `json:"apps"`
	}
	if err := json.Unmarshal([]byte(wappalyzer.GetFingerprints()), &raw); err != nil {
		t.Fatal(err)
	}
	for app, definition := range raw.Apps {
		for _, pattern := range definition.HTML {
			if literal := longestRequiredLiteral(pattern); len([]rune(literal)) < 4 {
				t.Logf("always HTML app=%q literal=%q pattern=%q", app, literal, pattern)
			}
		}
	}
}
