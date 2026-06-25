package tech

import (
	"sort"
	"strings"
	"testing"

	"cc-enrich-worker/internal/model"
)

// benchBody is a ~70KB realistic page: lots of filler + a few real tech signals.
var benchBody = []byte(strings.Repeat(`<div class="row"><p>lorem ipsum dolor sit amet consectetur</p></div>`, 1100) +
	`<meta name="generator" content="WordPress 6.4.2">` +
	`<link rel="stylesheet" href="/wp-content/themes/x/style.css">` +
	`<script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>` +
	strings.Repeat(`<script src="https://cdn.example.com/lib-`+`x.js"></script>`, 25))
var benchHeaders = map[string][]string{"Server": {"nginx"}}

func BenchmarkWappalyzer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = DetectTech(benchHeaders, benchBody) // fastTech nil in tests -> upstream
	}
}

func BenchmarkFast(b *testing.B) {
	fast, err := NewFastMatcher()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fast.Detect(benchHeaders, benchBody)
	}
}

func techNameSet(techs []model.Technology) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range techs {
		if !seen[t.Name] {
			seen[t.Name] = true
			out = append(out, t.Name)
		}
	}
	sort.Strings(out)
	return out
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFastMatcherParity(t *testing.T) {
	fast, err := NewFastMatcher()
	if err != nil {
		t.Fatal(err)
	}
	samples := []struct {
		name    string
		headers map[string][]string
		body    string
	}{
		{
			"wordpress+nginx+jquery",
			map[string][]string{"Server": {"nginx"}},
			`<html><head><meta name="generator" content="WordPress 6.4.2">` +
				`<link rel="stylesheet" href="/wp-content/themes/x/style.css">` +
				`<script src="https://code.jquery.com/jquery-3.6.0.min.js"></script></head><body>hi</body></html>`,
		},
		{
			"php-header-only",
			map[string][]string{"X-Powered-By": {"PHP/8.1.0"}},
			`<html><body>plain page</body></html>`,
		},
		{
			"cloudflare-header",
			map[string][]string{"Server": {"cloudflare"}, "CF-RAY": {"abc"}},
			`<html><body>nothing special here</body></html>`,
		},
		{
			"empty",
			map[string][]string{},
			`<html><body></body></html>`,
		},
		{
			"google-analytics-script",
			map[string][]string{},
			`<html><head><script src="https://www.googletagmanager.com/gtag/js?id=G-XXText"></script></head><body>x</body></html>`,
		},
		{
			"transitive-implies", // Melis -> Laravel -> PHP/MySQL/Symfony/Zend
			map[string][]string{},
			`<html><body><!-- rendered with melis cms v2 --></body></html>`,
		},
	}
	for _, s := range samples {
		want := techNameSet(DetectTech(s.headers, []byte(s.body)))
		got := techNameSet(fast.Detect(s.headers, []byte(s.body)))
		if !eqStrings(want, got) {
			t.Errorf("%s:\n  fast = %v\n  want = %v", s.name, got, want)
		}
	}
}
