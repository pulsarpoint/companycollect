package parse

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/markusmobius/go-trafilatura"
)

const boilerplatePage = `<html><head><title>Acme</title></head><body>
<nav><ul><li>Home</li><li>Products</li><li>Cookie settings</li></ul></nav>
<main><article>
<h1>Acme Industrial Pumps</h1>
<p>Acme designs and manufactures industrial pumps for the chemical, food and marine sectors.
Our engineering team has delivered custom pumping solutions since 1968, serving more than two
thousand customers across Europe. The quarterly results reflect continued growth in demand for
corrosion-resistant magnetic drive pumps and after-sales service contracts.</p>
<p>Beyond manufacturing, Acme operates a service network with certified technicians in twelve
countries, offering preventive maintenance programs, spare part logistics and on-site repairs
around the clock for critical process equipment.</p>
</article></main>
<footer><a href="/imprint">Imprint</a> <a href="/privacy">Privacy policy</a> © 2026 Acme</footer>
</body></html>`

func TestMainTextDropsBoilerplate(t *testing.T) {
	got := MainText([]byte(boilerplatePage), "https://acme.example/")
	if !strings.Contains(got, "quarterly results") {
		t.Fatalf("main content missing: %q", got)
	}
	if strings.Contains(got, "Cookie settings") || strings.Contains(got, "Privacy policy") {
		t.Fatalf("boilerplate leaked into embed text: %q", got)
	}
}

func TestMainTextFallsBackToWalk(t *testing.T) {
	// Too little content for trafilatura -> must fall back to the walk, never return less than today.
	tiny := `<html><body>Acme software company</body></html>`
	if got := MainText([]byte(tiny), "https://acme.example/"); !strings.Contains(got, "Acme software company") {
		t.Fatalf("fallback lost text: %q", got)
	}
}

func TestMainTextFallsBackWhenExtractorErrors(t *testing.T) {
	orig := extractMainContent
	defer func() { extractMainContent = orig }()
	extractMainContent = func(r io.Reader, opts trafilatura.Options) (*trafilatura.ExtractResult, error) {
		return nil, errors.New("extractor rejected page")
	}
	got := MainText([]byte(`<html><body>Acme software company</body></html>`), "https://acme.example/")
	if !strings.Contains(got, "Acme software company") {
		t.Fatalf("extractor error must degrade to walk text, got %q", got)
	}
}

func TestMainTextRecoversFromExtractorPanic(t *testing.T) {
	orig := extractMainContent
	defer func() { extractMainContent = orig }()
	extractMainContent = func(r io.Reader, opts trafilatura.Options) (*trafilatura.ExtractResult, error) {
		panic("pathological page")
	}
	got := MainText([]byte(`<html><body>Acme software company</body></html>`), "https://acme.example/")
	if !strings.Contains(got, "Acme software company") {
		t.Fatalf("extractor panic must degrade to walk text, got %q", got)
	}
}

func BenchmarkMainText(b *testing.B) {
	body := []byte(strings.Replace(boilerplatePage, "</article>", strings.Repeat("<p>Filler paragraph about pump engineering and service operations in industrial plants.</p>\n", 400)+"</article>", 1))
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MainText(body, "https://acme.example/")
	}
}

func BenchmarkWalkText(b *testing.B) {
	body := strings.Replace(boilerplatePage, "</article>", strings.Repeat("<p>Filler paragraph about pump engineering and service operations in industrial plants.</p>\n", 400)+"</article>", 1)
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParseHTML(body)
	}
}
