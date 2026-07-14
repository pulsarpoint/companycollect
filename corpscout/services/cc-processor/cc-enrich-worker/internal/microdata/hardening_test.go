package microdata

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// TestParseItemrefCycle pins the hardening this vendored copy adds over upstream
// github.com/iand/microdata v0.0.28: two itemscope nodes referencing each other via itemref
// recurse forever upstream (readItem guards only refnode != node, not indirect cycles) and
// crash the whole process with an unrecoverable stack overflow. Observed in production on
// CC-MAIN-2026-25. The parse must return, and the A->B->A expansion must not loop.
func TestParseItemrefCycle(t *testing.T) {
	page := `<html><body>
<div itemscope id="a" itemref="b" itemtype="https://schema.org/Organization"><span itemprop="name">A</span></div>
<div itemscope id="b" itemref="a" itemtype="https://schema.org/Organization"><span itemprop="name">B</span></div>
</body></html>`
	base, _ := url.Parse("https://example.com/")
	data, err := NewParser(strings.NewReader(page), base).Parse()
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(data.Items) != 2 {
		t.Fatalf("items = %d, want 2", len(data.Items))
	}
}

// TestParseDeepNesting pins the layered depth protection: html.Parse itself rejects documents
// with more than 512 open elements ("open stack of elements exceeds 512 nodes"), so pathological
// DOM nesting surfaces as an ordinary error, never a crash. (The readItem depth cap in this
// package guards the OTHER unbounded path — long itemref chains, which add recursion depth
// without DOM depth.)
func TestParseDeepNesting(t *testing.T) {
	var b strings.Builder
	b.WriteString(`<html><body><div itemscope itemtype="https://schema.org/Organization">`)
	for range 100000 {
		b.WriteString("<div>")
	}
	b.WriteString(`<span itemprop="name">deep</span>`)
	b.WriteString(`</body></html>`)
	base, _ := url.Parse("https://example.com/")
	if _, err := NewParser(strings.NewReader(b.String()), base).Parse(); err == nil {
		t.Fatal("expected html.Parse depth error for 100k-deep nesting, got nil")
	}
}

// TestParseLongItemrefChain pins the readItem depth cap: a long NON-cyclic itemref chain adds a
// recursion frame per link with no DOM nesting, so only maxReadItemDepth bounds it. The parse
// must return without error; links beyond the cap are dropped.
func TestParseLongItemrefChain(t *testing.T) {
	var b strings.Builder
	b.WriteString(`<html><body>`)
	b.WriteString(`<div itemscope id="n0" itemref="n1" itemtype="https://schema.org/Organization"><span itemprop="name">root</span></div>`)
	for i := 1; i < 2000; i++ {
		fmt.Fprintf(&b, `<div itemscope id="n%d" itemref="n%d"><span itemprop="name">x</span></div>`, i, i+1)
	}
	b.WriteString(`</body></html>`)
	base, _ := url.Parse("https://example.com/")
	if _, err := NewParser(strings.NewReader(b.String()), base).Parse(); err != nil {
		t.Fatalf("Parse: %v", err)
	}
}
