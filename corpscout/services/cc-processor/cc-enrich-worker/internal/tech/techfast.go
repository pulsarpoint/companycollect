package tech

import (
	"bytes"
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	ahocorasick "github.com/petar-dambovaliev/aho-corasick"
	wappalyzer "github.com/projectdiscovery/wappalyzergo"
	"golang.org/x/net/html"

	"cc-enrich-worker/internal/model"
)

type fpat struct {
	app   string
	pat   *wappalyzer.ParsedPattern
	match *regexp.Regexp // capture-free twin of pat's regex; MatchString skips submatch tracking
}

// wappalyzergo's version-capture rewrites (patterns.go ParsePattern), replicated verbatim so the
// match-only twin compiles from exactly the same regex source as ParsedPattern's internal one.
const (
	wapVerCap1        = `(\d+(?:\.\d+)+)`
	wapVerCap1Fill    = "__verCap1__"
	wapVerCap1Limited = `(\d{1,20}(?:\.\d{1,20}){1,20})`

	wapVerCap2        = `((?:\d+\.)+\d+)`
	wapVerCap2Fill    = "__verCap2__"
	wapVerCap2Limited = `((?:\d{1,20}\.){1,20}\d{1,20})`
)

// matchOnlyRegex compiles the same regex ParsePattern builds internally, for use with
// MatchString: Go's regexp only runs the slow capture-tracking machine for submatch calls, so
// rejecting (the overwhelmingly common case for gated patterns) through this twin avoids it.
// Returns nil for empty (SkipRegex) or uncompilable patterns — callers then use pat.Evaluate.
func matchOnlyRegex(pattern string) *regexp.Regexp {
	parts := strings.Split(pattern, "\\;")
	if parts[0] == "" {
		return nil
	}
	r := parts[0]
	r = strings.ReplaceAll(r, wapVerCap1, wapVerCap1Fill)
	r = strings.ReplaceAll(r, wapVerCap2, wapVerCap2Fill)
	r = strings.ReplaceAll(r, "\\+", "__escapedPlus__")
	r = strings.ReplaceAll(r, "+", "{1,250}")
	r = strings.ReplaceAll(r, "*", "{0,250}")
	r = strings.ReplaceAll(r, "__escapedPlus__", "\\+")
	r = strings.ReplaceAll(r, wapVerCap1Fill, wapVerCap1Limited)
	r = strings.ReplaceAll(r, wapVerCap2Fill, wapVerCap2Limited)
	re, err := regexp.Compile("(?i)" + r)
	if err != nil {
		return nil
	}
	return re
}

// newFpat parses a wappalyzer pattern and builds its match-only twin. ok is false when the
// pattern cannot be parsed at all (upstream would drop it too).
func newFpat(app, pattern string) (fpat, bool) {
	pp := mustParse(pattern)
	if pp == nil {
		return fpat{}, false
	}
	return fpat{app: app, pat: pp, match: matchOnlyRegex(pattern)}, true
}

// FastMatcher reimplements wappalyzergo's detection orchestration (headers → cookies →
// html/script/meta body) but gates the expensive HTML-body patterns with Aho-Corasick:
// only patterns whose required literal is present in the body get their regex evaluated.
// It reuses wappalyzergo's ParsePattern/Evaluate (exact matching + version extraction) and
// category mapping; headers/cookies/meta evaluate the same patterns against the same inputs
// as upstream (cookies: pattern vs the cookie VALUE, see normalizeCookies). Confidence follows
// upstream's two-level aggregation: max within one evidence scope, then sum across scopes capped
// at 100. Detections remain a SUPERSET of upstream's because implies are applied transitively.
type FastMatcher struct {
	ac         ahocorasick.AhoCorasick
	litToPats  [][]fpat // index-aligned with the AC dictionary (HTML body)
	alwaysHTML []fpat   // HTML patterns with no extractable required literal

	acScript        ahocorasick.AhoCorasick // gate for <script src> patterns
	litToScriptPats [][]fpat
	alwaysScript    []fpat

	metaPats   map[string][]fpat   // meta name (lower) -> patterns
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
	sort.Strings(out)
	return out
}

func mustParse(p string) *wappalyzer.ParsedPattern {
	pp, err := wappalyzer.ParsePattern(p)
	if err != nil {
		return nil
	}
	return pp
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
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

	for _, app := range sortedKeys(raw.Apps) {
		a := raw.Apps[app]
		for _, c := range a.Cats {
			if ci, ok := cats[c]; ok {
				m.appCats[app] = append(m.appCats[app], ci.Name)
			}
		}
		if imp := parseImplies(a.Implies); len(imp) > 0 {
			m.appImplies[app] = imp
		}
		for _, p := range a.HTML {
			fp, ok := newFpat(app, p)
			if !ok {
				continue
			}
			if lits := gateLiterals(p); len(lits) > 0 { // any-of gate: pattern registered under every literal
				for _, lit := range lits {
					litMap[lit] = append(litMap[lit], fp)
				}
			} else {
				m.alwaysHTML = append(m.alwaysHTML, fp)
			}
		}
		for _, p := range a.ScriptSrc {
			fp, ok := newFpat(app, p)
			if !ok {
				continue
			}
			if lits := gateLiterals(p); len(lits) > 0 {
				for _, lit := range lits {
					scriptLitMap[lit] = append(scriptLitMap[lit], fp)
				}
			} else {
				m.alwaysScript = append(m.alwaysScript, fp)
			}
		}
		for _, name := range sortedKeys(a.Meta) {
			pats := a.Meta[name]
			ln := strings.ToLower(name)
			for _, p := range pats {
				if fp, ok := newFpat(app, p); ok {
					m.metaPats[ln] = append(m.metaPats[ln], fp)
				}
			}
		}
		for _, name := range sortedKeys(a.Headers) {
			if fp, ok := newFpat(app, a.Headers[name]); ok {
				ln := strings.ToLower(name)
				m.headerPats[ln] = append(m.headerPats[ln], fp)
			}
		}
		for _, name := range sortedKeys(a.Cookies) {
			if fp, ok := newFpat(app, a.Cookies[name]); ok {
				ln := strings.ToLower(name)
				m.cookiePats[ln] = append(m.cookiePats[ln], fp)
			}
		}
	}

	opts := ahocorasick.Opts{
		MatchKind: ahocorasick.StandardMatch, // report every occurrence (overlapping iter)
		DFA:       true,
	}

	dict := sortedKeys(litMap)
	m.litToPats = make([][]fpat, len(dict))
	for i, lit := range dict {
		m.litToPats[i] = litMap[lit]
	}
	htmlBuilder := ahocorasick.NewAhoCorasickBuilder(opts)
	m.ac = htmlBuilder.Build(dict)

	sdict := sortedKeys(scriptLitMap)
	m.litToScriptPats = make([][]fpat, len(sdict))
	for i, lit := range sdict {
		m.litToScriptPats[i] = scriptLitMap[lit]
	}
	scriptBuilder := ahocorasick.NewAhoCorasickBuilder(opts)
	m.acScript = scriptBuilder.Build(sdict)
	return m, nil
}

// normalizeCookies mirrors upstream wappalyzergo (findSetCookie + normalizeCookies): lowercase each
// Set-Cookie header, split on spaces, split each piece on commas (or else semicolons), and keep
// name=value pairs. Crude — a value containing spaces/commas fragments — but upstream parity IS the
// contract here: the fingerprint patterns are then evaluated against the cookie value only.
func normalizeCookies(setCookies []string) map[string]string {
	if len(setCookies) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, sc := range setCookies {
		for _, v := range strings.Split(strings.ToLower(sc), " ") {
			if v == "" {
				continue
			}
			var frags []string
			switch {
			case strings.Contains(v, ","):
				frags = strings.Split(v, ",")
			case strings.Contains(v, ";"):
				frags = strings.Split(v, ";")
			default:
				frags = []string{v}
			}
			for _, f := range frags {
				parts := strings.SplitN(strings.Trim(f, " "), "=", 2)
				if len(parts) < 2 {
					continue
				}
				out[parts[0]] = parts[1]
			}
		}
	}
	return out
}

type scopedMatch struct {
	confidence int
	version    string
}

type detectedTechnology struct {
	confidence int
	version    string
}

// recordScoped keeps one application's strongest evidence from a single matcher scope. This is
// how upstream treats all headers, all cookies, the whole HTML body, or one script/meta element:
// confidence is the maximum matching pattern confidence, not the sum of every matching regex.
func recordScoped(matches map[string]scopedMatch, fp fpat, target string) {
	var matched bool
	var version string
	switch {
	case fp.match == nil: // SkipRegex or no twin — upstream evaluation path
		matched, version = fp.pat.Evaluate(target)
	case !fp.match.MatchString(target):
		return // capture-free reject: the overwhelmingly common outcome for gated patterns
	case fp.pat.Version == "":
		matched = true // real hit, no version template — nothing left to extract
	default:
		matched, version = fp.pat.Evaluate(target) // real hit: pay the submatch machine for the version
	}
	if !matched {
		return
	}
	current, exists := matches[fp.app]
	if !exists || fp.pat.Confidence > current.confidence {
		current.confidence = fp.pat.Confidence
	}
	if betterVersion(version, current.version) {
		current.version = version
	}
	matches[fp.app] = current
}

func addDetected(found map[string]detectedTechnology, app string, confidence int, version string) {
	current := found[app]
	if confidence > 0 {
		current.confidence += confidence
		if current.confidence > 100 {
			current.confidence = 100
		}
	}
	// Across scopes, upstream keeps the first non-empty version. Scope order below is fixed, and
	// an implied empty version never prevents a later direct version from filling this field.
	if current.version == "" && version != "" {
		current.version = version
	}
	found[app] = current
}

func (m *FastMatcher) addImplications(found map[string]detectedTechnology, app string, confidence int, seen map[string]bool) {
	for _, implied := range m.appImplies[app] {
		if seen[implied] {
			continue
		}
		seen[implied] = true
		addDetected(found, implied, confidence, "")
		m.addImplications(found, implied, confidence, seen)
	}
}

func (m *FastMatcher) mergeScope(found map[string]detectedTechnology, matches map[string]scopedMatch) {
	for _, app := range sortedKeys(matches) {
		match := matches[app]
		addDetected(found, app, match.confidence, match.version)
		m.addImplications(found, app, match.confidence, map[string]bool{app: true})
	}
}

func normalizeHeaders(headers map[string][]string) map[string]string {
	normalized := make(map[string]string, len(headers))
	for _, name := range sortedKeys(headers) {
		lowerName := strings.ToLower(name)
		value := strings.ToLower(strings.Join(headers[name], ", "))
		if previous := normalized[lowerName]; previous != "" {
			if value == "" {
				value = previous
			} else {
				value = previous + ", " + value
			}
		}
		normalized[lowerName] = value
	}
	return normalized
}

// Detect mirrors wappalyzer.Fingerprint: lowercase, then headers + cookies + body.
func (m *FastMatcher) Detect(headers map[string][]string, body []byte) []model.Technology {
	normBody := bytes.ToLower(body)
	bodyStr := string(normBody)
	found := map[string]detectedTechnology{}
	normalizedHeaders := normalizeHeaders(headers)

	// Upstream evaluates all normalized headers as one evidence scope.
	headerMatches := map[string]scopedMatch{}
	for _, name := range sortedKeys(normalizedHeaders) {
		for _, fp := range m.headerPats[name] {
			recordScoped(headerMatches, fp, normalizedHeaders[name])
		}
	}
	m.mergeScope(found, headerMatches)

	// Cookies: mirror upstream exactly (fingerprint_cookies.go) — normalize Set-Cookie into
	// name->value and evaluate each pattern against the VALUE only. Evaluating against the whole
	// "name=value; attrs" string dead-ends every anchored value pattern (^\d+$, ^\w+$, …).
	cookieMatches := map[string]scopedMatch{}
	cookies := normalizeCookies([]string{normalizedHeaders["set-cookie"]})
	for _, name := range sortedKeys(cookies) {
		value := cookies[name]
		for _, fp := range m.cookiePats[name] {
			recordScoped(cookieMatches, fp, value)
		}
	}
	m.mergeScope(found, cookieMatches)

	// HTML body — Aho-Corasick gate: only patterns whose required literal is present. A pattern
	// gated by several literals (any-of alternation gates) must still evaluate only once.
	htmlMatches := map[string]scopedMatch{}
	seen := make(map[int]bool)
	evaluated := make(map[*wappalyzer.ParsedPattern]bool)
	iter := m.ac.IterOverlapping(bodyStr)
	for match := iter.Next(); match != nil; match = iter.Next() {
		idx := match.Pattern()
		if seen[idx] {
			continue
		}
		seen[idx] = true
		for _, fp := range m.litToPats[idx] {
			if evaluated[fp.pat] {
				continue
			}
			evaluated[fp.pat] = true
			recordScoped(htmlMatches, fp, bodyStr)
		}
	}
	for _, fp := range m.alwaysHTML {
		recordScoped(htmlMatches, fp, bodyStr)
	}
	m.mergeScope(found, htmlMatches)

	// Each script src and each meta element is a separate evidence scope, in document order.
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
				scriptMatches := map[string]scopedMatch{}
				sseen := make(map[int]bool)
				si := m.acScript.IterOverlapping(src)
				for match := si.Next(); match != nil; match = si.Next() {
					idx := match.Pattern()
					if sseen[idx] {
						continue
					}
					sseen[idx] = true
					for _, fp := range m.litToScriptPats[idx] {
						recordScoped(scriptMatches, fp, src)
					}
				}
				for _, fp := range m.alwaysScript {
					recordScoped(scriptMatches, fp, src)
				}
				m.mergeScope(found, scriptMatches)
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
			metaMatches := map[string]scopedMatch{}
			for _, fp := range m.metaPats[name] {
				recordScoped(metaMatches, fp, content)
			}
			m.mergeScope(found, metaMatches)
		}
	}

	out := make([]model.Technology, 0, len(found))
	for _, app := range sortedKeys(found) {
		detection := found[app]
		if detection.confidence == 0 {
			continue
		}
		cat := ""
		if cs := m.appCats[app]; len(cs) > 0 {
			cat = cs[0]
		}
		out = append(out, model.Technology{
			Name: app, Category: cat, Version: detection.version, Confidence: uint8(detection.confidence),
		})
	}
	return out
}
