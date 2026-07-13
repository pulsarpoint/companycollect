package tech

import "testing"

// TestMatchFirstEvaluationParity pins recordScoped's match-first fast path to upstream
// ParsedPattern.Evaluate semantics: same match verdicts, same extracted versions, same
// confidences — across plain patterns, version templates, confidence suffixes, and the
// +/* rewrites ParsePattern performs.
func TestMatchFirstEvaluationParity(t *testing.T) {
	cases := []struct {
		name, pattern, target string
		wantMatch             bool
		wantVersion           string
		wantConfidence        int
	}{
		{"plain match", `wp-content`, `<link href="/wp-content/themes/x.css">`, true, "", 100},
		{"plain miss", `wp-content`, `<p>nothing to see</p>`, false, "", 0},
		{"version extract", `jquery-([\d.]+)\.js\;version:\1`, `<script src="/jquery-3.6.0.js">`, true, "3.6.0", 100},
		{"version miss", `jquery-([\d.]+)\.js\;version:\1`, `<script src="/react.js">`, false, "", 0},
		{"confidence suffix", `cdn\.shopify\.com\;confidence:50`, `img src="//cdn.shopify.com/x.png"`, true, "", 50},
		{"star rewrite", `data-react\S{0,9}root\;confidence:25`, `<div data-reactroot></div>`, true, "", 25},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fp, ok := newFpat("App", tc.pattern)
			if !ok {
				t.Fatalf("newFpat(%q) failed to build", tc.pattern)
			}
			// The fixture's expectations must agree with upstream Evaluate — guards fixture rot.
			if em, ev := fp.pat.Evaluate(tc.target); em != tc.wantMatch || ev != tc.wantVersion {
				t.Fatalf("fixture disagrees with upstream Evaluate: got (%v,%q), want (%v,%q)",
					em, ev, tc.wantMatch, tc.wantVersion)
			}
			matches := map[string]scopedMatch{}
			recordScoped(matches, fp, tc.target)
			got, found := matches["App"]
			if found != tc.wantMatch {
				t.Fatalf("recordScoped matched=%v, want %v (matches=%+v)", found, tc.wantMatch, matches)
			}
			if found && (got.version != tc.wantVersion || got.confidence != tc.wantConfidence) {
				t.Fatalf("recordScoped = {conf:%d ver:%q}, want {conf:%d ver:%q}",
					got.confidence, got.version, tc.wantConfidence, tc.wantVersion)
			}
		})
	}
}
