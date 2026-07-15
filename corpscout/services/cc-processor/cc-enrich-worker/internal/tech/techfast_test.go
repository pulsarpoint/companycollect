package tech

import (
	"reflect"
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

// upstream is the parity oracle: the FastMatcher tests assert equivalence against the
// upstream wappalyzergo full scan.
var upstream = func() *Wappalyzer {
	wz, err := NewWappalyzer()
	if err != nil {
		panic(err)
	}
	return wz
}()

func BenchmarkWappalyzer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = upstream.Detect(benchHeaders, benchBody)
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

func technologyNamed(technologies []model.Technology, name string) (model.Technology, bool) {
	for _, technology := range technologies {
		if technology.Name == name {
			return technology, true
		}
	}
	return model.Technology{}, false
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
		wantTechnologies := upstream.Detect(s.headers, []byte(s.body))
		gotTechnologies := fast.Detect(s.headers, []byte(s.body))
		want := techNameSet(wantTechnologies)
		got := techNameSet(gotTechnologies)
		if !eqStrings(want, got) {
			t.Errorf("%s:\n  fast = %v\n  want = %v", s.name, got, want)
			continue
		}
		for _, wantTechnology := range wantTechnologies {
			gotTechnology, found := technologyNamed(gotTechnologies, wantTechnology.Name)
			if !found {
				continue
			}
			if gotTechnology.Version != wantTechnology.Version || gotTechnology.Category != wantTechnology.Category {
				t.Errorf("%s %s: fast version/category = %q/%q, want %q/%q",
					s.name, wantTechnology.Name, gotTechnology.Version, gotTechnology.Category,
					wantTechnology.Version, wantTechnology.Category)
			}
		}
	}
}

// Set-Cookie parity: upstream evaluates cookie patterns against the cookie VALUE only
// (wappalyzergo fingerprint_cookies.go normalizeCookies) — never against the whole
// "name=value; attrs" string, where every anchored value pattern (^\d+$, ^\w+$, …) is dead on
// arrival. Also covers the empty-pattern "cookie exists" fingerprints (430 of 468 in the JSON).
func TestFastMatcherCookieParity(t *testing.T) {
	fast, err := NewFastMatcher()
	if err != nil {
		t.Fatal(err)
	}
	samples := []struct {
		name    string
		headers map[string][]string
	}{
		// empty pattern -> existence check (PHP via PHPSESSID)
		{"existence-phpsessid", map[string][]string{"Set-Cookie": {"PHPSESSID=abc123; path=/"}}},
		// anchored ^\w+$ against the value (SoteShop -> implies PHP)
		{"anchored-word-soteshop", map[string][]string{"Set-Cookie": {"soteshop=abc123; path=/; HttpOnly"}}},
		// anchored ^\d+$ against the value (DigitalRiver)
		{"anchored-digits-x-dr-theme", map[string][]string{"Set-Cookie": {"x-dr-theme=42; path=/; HttpOnly"}}},
		// anchored ^\d+$ must NOT match a non-digit value (no Retail Rocket)
		{"anchored-digits-value-mismatch", map[string][]string{"Set-Cookie": {"rrpvid=notdigits; max-age=3600"}}},
		// two Set-Cookie headers in one response
		{"two-cookies", map[string][]string{"Set-Cookie": {"laravel_session=xyz; path=/", "x-dr-theme=7"}}},
	}
	body := []byte(`<html><body>plain</body></html>`)
	for _, s := range samples {
		want := techNameSet(upstream.Detect(s.headers, body))
		got := techNameSet(fast.Detect(s.headers, body))
		if !eqStrings(want, got) {
			t.Errorf("%s:\n  fast = %v\n  want = %v", s.name, got, want)
		}
	}
}

func TestFastMatcherSuppressesConfidenceZeroEvidence(t *testing.T) {
	fast, err := NewFastMatcher()
	if err != nil {
		t.Fatal(err)
	}

	got := fast.Detect(nil, []byte(`<meta name="data-version" content="9.12.0">`))
	if technology, found := technologyNamed(got, "Atlassian Jira"); found {
		t.Fatalf("confidence-zero Jira evidence must not detect Jira: %+v", technology)
	}
	if technology, found := technologyNamed(got, "Java"); found {
		t.Fatalf("confidence-zero Jira evidence must not imply Java: %+v", technology)
	}
}

func TestFastMatcherConfidenceZeroEvidenceCanSupplyVersion(t *testing.T) {
	fast, err := NewFastMatcher()
	if err != nil {
		t.Fatal(err)
	}

	bodies := [][]byte{
		[]byte(`<meta name="application-name" content="jira"><meta name="data-version" content="9.12.0">`),
		[]byte(`<meta name="data-version" content="9.12.0"><meta name="application-name" content="jira">`),
	}
	for _, body := range bodies {
		jira, found := technologyNamed(fast.Detect(nil, body), "Atlassian Jira")
		if !found {
			t.Fatalf("positive Jira evidence was not detected for %q", body)
		}
		if jira.Version != "9.12.0" || jira.Confidence != 100 {
			t.Fatalf("Jira evidence aggregation = %+v, want version 9.12.0 confidence 100", jira)
		}
	}
}

func TestFastMatcherUsesMaxConfidenceWithinScopeAndSumAcrossScopes(t *testing.T) {
	fast, err := NewFastMatcher()
	if err != nil {
		t.Fatal(err)
	}

	govukBody := []byte(`<html><body class="govuk-template__body"><a class="govuk-link">Link</a></body></html>`)
	govuk, found := technologyNamed(fast.Detect(nil, govukBody), "GOV.UK Frontend")
	if !found || govuk.Confidence != 80 {
		t.Fatalf("same-scope confidence = %+v, want GOV.UK Frontend confidence 80", govuk)
	}

	odooBody := []byte(`<link href="/web/css/web.assets_common/x.css"><script src="/web/js/web.assets_common/x.js"></script>`)
	odoo, found := technologyNamed(fast.Detect(nil, odooBody), "Odoo")
	if !found || odoo.Confidence != 50 {
		t.Fatalf("cross-scope confidence = %+v, want Odoo confidence 50", odoo)
	}
}

func TestFastMatcherHeaderOrderIsDeterministicAndDirectVersionSurvivesImplication(t *testing.T) {
	fast, err := NewFastMatcher()
	if err != nil {
		t.Fatal(err)
	}

	headersA := map[string][]string{}
	headersA["Server"] = []string{"PHP/8.2.4"}
	headersA["X-Powered-By"] = []string{"MODX"}
	headersB := map[string][]string{}
	headersB["X-Powered-By"] = []string{"MODX"}
	headersB["Server"] = []string{"PHP/8.2.4"}
	body := []byte(`<html><body>plain</body></html>`)

	gotA := fast.Detect(headersA, body)
	gotB := fast.Detect(headersB, body)
	if !reflect.DeepEqual(gotA, gotB) {
		t.Fatalf("header insertion order changed output:\nA=%+v\nB=%+v", gotA, gotB)
	}
	php, found := technologyNamed(gotA, "PHP")
	if !found || php.Version != "8.2.4" || php.Confidence != 100 {
		t.Fatalf("direct PHP version was lost after MODX implication: %+v", php)
	}
	for index := 1; index < len(gotA); index++ {
		if gotA[index-1].Name > gotA[index].Name {
			t.Fatalf("technology output is not sorted: %+v", gotA)
		}
	}
}

func TestFastMatcherConstructionIsDeterministic(t *testing.T) {
	headers := map[string][]string{"Server": {"PHP/8.2.4"}, "X-Powered-By": {"MODX"}}
	body := []byte(`<meta name="application-name" content="jira"><meta name="data-version" content="9.12.0">`)

	first, err := NewFastMatcher()
	if err != nil {
		t.Fatal(err)
	}
	want := first.Detect(headers, body)
	second, err := NewFastMatcher()
	if err != nil {
		t.Fatal(err)
	}
	if got := second.Detect(headers, body); !reflect.DeepEqual(got, want) {
		t.Fatalf("matcher construction changed output:\nfirst=%+v\nsecond=%+v", want, got)
	}
}
