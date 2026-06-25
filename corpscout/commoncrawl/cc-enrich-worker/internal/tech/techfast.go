package tech

import (
	"bytes"
	"encoding/json"
	"strings"

	ahocorasick "github.com/petar-dambovaliev/aho-corasick"
	wappalyzer "github.com/projectdiscovery/wappalyzergo"
	"golang.org/x/net/html"

	"cc-enrich-worker/internal/model"
)

type fpat struct {
	app string
	pat *wappalyzer.ParsedPattern
}

// FastMatcher reimplements wappalyzergo's detection orchestration (headers → cookies →
// html/script/meta body) but gates the expensive HTML-body patterns with Aho-Corasick:
// only patterns whose required literal is present in the body get their regex evaluated.
// It reuses wappalyzergo's ParsePattern/Evaluate (exact matching + version extraction)
// and category mapping, so non-HTML parts are identical to upstream.
type FastMatcher struct {
	ac         ahocorasick.AhoCorasick
	litToPats  [][]fpat // index-aligned with the AC dictionary (HTML body)
	alwaysHTML []fpat   // HTML patterns with no extractable required literal

	acScript        ahocorasick.AhoCorasick // gate for <script src> patterns
	litToScriptPats [][]fpat
	alwaysScript    []fpat

	metaPats   map[string][]fpat // meta name (lower) -> patterns
	headerPats map[string][]fpat   // header name (lower) -> patterns
	cookiePats map[string][]fpat   // cookie name (lower) -> patterns
	appCats    map[string][]string // app -> category names
	appImplies map[string][]string
}

type rawApp struct {
	Cats      []int               `json:"cats"`
	HTML      []string            `json:"html"`
	ScriptSrc []string            `json:"scriptSrc"`
	Meta      map[string][]string `json:"meta"`
	Headers   map[string]string   `json:"headers"`
	Cookies   map[string]string   `json:"cookies"`
	Implies   json.RawMessage     `json:"implies"` // string OR []string in the JSON
}

// parseImplies handles both "implies": "PHP" and "implies": ["PHP","MySQL"], stripping
// the wappalyzer "\;confidence:.." suffix so the implied name matches the Apps map.
func parseImplies(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var arr []string
	if err := json.Unmarshal(raw, &arr); err != nil {
		var one string
		if json.Unmarshal(raw, &one) != nil {
			return nil
		}
		arr = []string{one}
	}
	out := make([]string, 0, len(arr))
	for _, s := range arr {
		if i := strings.Index(s, `\;`); i >= 0 {
			s = s[:i]
		}
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func mustParse(p string) *wappalyzer.ParsedPattern {
	pp, err := wappalyzer.ParsePattern(p)
	if err != nil {
		return nil
	}
	return pp
}

func NewFastMatcher() (*FastMatcher, error) {
	var raw struct {
		Apps map[string]rawApp `json:"apps"`
	}
	if err := json.Unmarshal([]byte(wappalyzer.GetFingerprints()), &raw); err != nil {
		return nil, err
	}
	cats := wappalyzer.GetCategoriesMapping()

	m := &FastMatcher{
		metaPats:   map[string][]fpat{},
		headerPats: map[string][]fpat{},
		cookiePats: map[string][]fpat{},
		appCats:    map[string][]string{},
		appImplies: map[string][]string{},
	}
	litMap := map[string][]fpat{}       // required literal -> html patterns
	scriptLitMap := map[string][]fpat{} // required literal -> scriptSrc patterns

	for app, a := range raw.Apps {
		for _, c := range a.Cats {
			if ci, ok := cats[c]; ok {
				m.appCats[app] = append(m.appCats[app], ci.Name)
			}
		}
		if imp := parseImplies(a.Implies); len(imp) > 0 {
			m.appImplies[app] = imp
		}
		for _, p := range a.HTML {
			pp := mustParse(p)
			if pp == nil {
				continue
			}
			fp := fpat{app, pp}
			if lit := longestRequiredLiteral(p); len([]rune(lit)) >= 4 {
				litMap[lit] = append(litMap[lit], fp)
			} else {
				m.alwaysHTML = append(m.alwaysHTML, fp)
			}
		}
		for _, p := range a.ScriptSrc {
			pp := mustParse(p)
			if pp == nil {
				continue
			}
			fp := fpat{app, pp}
			if lit := longestRequiredLiteral(p); len([]rune(lit)) >= 4 {
				scriptLitMap[lit] = append(scriptLitMap[lit], fp)
			} else {
				m.alwaysScript = append(m.alwaysScript, fp)
			}
		}
		for name, pats := range a.Meta {
			ln := strings.ToLower(name)
			for _, p := range pats {
				if pp := mustParse(p); pp != nil {
					m.metaPats[ln] = append(m.metaPats[ln], fpat{app, pp})
				}
			}
		}
		for name, p := range a.Headers {
			if pp := mustParse(p); pp != nil {
				ln := strings.ToLower(name)
				m.headerPats[ln] = append(m.headerPats[ln], fpat{app, pp})
			}
		}
		for name, p := range a.Cookies {
			if pp := mustParse(p); pp != nil {
				ln := strings.ToLower(name)
				m.cookiePats[ln] = append(m.cookiePats[ln], fpat{app, pp})
			}
		}
	}

	opts := ahocorasick.Opts{
		MatchKind: ahocorasick.StandardMatch, // report every occurrence (overlapping iter)
		DFA:       true,
	}

	dict := make([]string, 0, len(litMap))
	for lit := range litMap {
		dict = append(dict, lit)
	}
	m.litToPats = make([][]fpat, len(dict))
	for i, lit := range dict {
		m.litToPats[i] = litMap[lit]
	}
	htmlBuilder := ahocorasick.NewAhoCorasickBuilder(opts)
	m.ac = htmlBuilder.Build(dict)

	sdict := make([]string, 0, len(scriptLitMap))
	for lit := range scriptLitMap {
		sdict = append(sdict, lit)
	}
	m.litToScriptPats = make([][]fpat, len(sdict))
	for i, lit := range sdict {
		m.litToScriptPats[i] = scriptLitMap[lit]
	}
	scriptBuilder := ahocorasick.NewAhoCorasickBuilder(opts)
	m.acScript = scriptBuilder.Build(sdict)
	return m, nil
}

// Detect mirrors wappalyzer.Fingerprint: lowercase, then headers + cookies + body.
func (m *FastMatcher) Detect(headers map[string][]string, body []byte) []model.Technology {
	normBody := bytes.ToLower(body)
	bodyStr := string(normBody)
	found := map[string]string{} // app -> version (first match wins, "" if none)

	var record func(app, ver string)
	record = func(app, ver string) {
		if _, ok := found[app]; ok {
			return
		}
		found[app] = ver
		for _, imp := range m.appImplies[app] {
			record(imp, "") // transitive; found map guards cycles
		}
	}
	eval := func(fp fpat, target string) {
		if ok, ver := fp.pat.Evaluate(target); ok {
			record(fp.app, ver)
		}
	}

	for k, vals := range headers {
		pats, ok := m.headerPats[strings.ToLower(k)]
		if !ok {
			continue
		}
		for _, v := range vals {
			lv := strings.ToLower(v)
			for _, fp := range pats {
				eval(fp, lv)
			}
		}
	}
	for _, c := range headers["Set-Cookie"] {
		name := strings.ToLower(strings.TrimSpace(strings.SplitN(c, "=", 2)[0]))
		for _, fp := range m.cookiePats[name] {
			eval(fp, strings.ToLower(c))
		}
	}

	// HTML body — Aho-Corasick gate: only patterns whose required literal is present.
	seen := make(map[int]bool)
	iter := m.ac.IterOverlapping(bodyStr)
	for match := iter.Next(); match != nil; match = iter.Next() {
		idx := match.Pattern()
		if seen[idx] {
			continue
		}
		seen[idx] = true
		for _, fp := range m.litToPats[idx] {
			eval(fp, bodyStr)
		}
	}
	for _, fp := range m.alwaysHTML {
		eval(fp, bodyStr)
	}

	// scriptSrc + meta from the (lowercased) tokenized body, as wappalyzer does.
	tok := html.NewTokenizer(bytes.NewReader(normBody))
	for {
		tt := tok.Next()
		if tt == html.ErrorToken {
			break
		}
		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}
		t := tok.Token()
		switch t.Data {
		case "script":
			for _, a := range t.Attr {
				if a.Key != "src" {
					continue
				}
				src := a.Val // already lowercased (body was lowercased)
				sseen := make(map[int]bool)
				si := m.acScript.IterOverlapping(src)
				for match := si.Next(); match != nil; match = si.Next() {
					idx := match.Pattern()
					if sseen[idx] {
						continue
					}
					sseen[idx] = true
					for _, fp := range m.litToScriptPats[idx] {
						eval(fp, src)
					}
				}
				for _, fp := range m.alwaysScript {
					eval(fp, src)
				}
			}
		case "meta":
			var name, content string
			for _, a := range t.Attr {
				switch a.Key {
				case "name":
					name = a.Val
				case "content":
					content = a.Val
				}
			}
			for _, fp := range m.metaPats[strings.ToLower(name)] {
				eval(fp, content)
			}
		}
	}

	out := make([]model.Technology, 0, len(found))
	for app, ver := range found {
		cat := ""
		if cs := m.appCats[app]; len(cs) > 0 {
			cat = cs[0]
		}
		out = append(out, model.Technology{Name: app, Category: cat, Version: ver})
	}
	return out
}
