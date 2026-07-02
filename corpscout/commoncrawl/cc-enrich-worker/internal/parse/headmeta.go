package parse

import (
	"strings"

	"golang.org/x/net/html"
)

// HeadMeta is the HTML-head content-signal set captured per page (feeds commoncrawl_domain_page_meta).
type HeadMeta struct {
	Title     string
	Meta      map[string]string // <meta name|property> -> content, key lowercased, first value wins
	Canonical string
	Hreflang  []string // declared alternate languages/regions (lowercased, deduped)
	Charset   string
}

// ParseHeadMeta walks the document for <title>, every <meta name|property ... content>, <link
// rel=canonical>, <link rel=alternate hreflang>, and <meta charset>. It relies on x/net/html's lenient
// parsing, so malformed HTML yields best-effort values rather than an error; anything absent stays zero.
func ParseHeadMeta(htmlStr string) HeadMeta {
	hm := HeadMeta{Meta: map[string]string{}}
	doc, err := html.Parse(strings.NewReader(htmlStr))
	if err != nil {
		return hm
	}
	seenLang := map[string]bool{}
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "title":
				if hm.Title == "" && n.FirstChild != nil && n.FirstChild.Type == html.TextNode {
					hm.Title = strings.TrimSpace(n.FirstChild.Data)
				}
			case "meta":
				var name, property, content, charset string
				for _, a := range n.Attr {
					switch strings.ToLower(a.Key) {
					case "name":
						name = a.Val
					case "property":
						property = a.Val
					case "content":
						content = a.Val
					case "charset":
						charset = a.Val
					}
				}
				if charset != "" && hm.Charset == "" {
					hm.Charset = strings.ToLower(strings.TrimSpace(charset))
				}
				key := name
				if key == "" {
					key = property
				}
				if key != "" && content != "" {
					k := strings.ToLower(strings.TrimSpace(key))
					if _, ok := hm.Meta[k]; !ok {
						hm.Meta[k] = content
					}
				}
			case "link":
				var rel, href, hreflang string
				for _, a := range n.Attr {
					switch strings.ToLower(a.Key) {
					case "rel":
						rel = strings.ToLower(a.Val)
					case "href":
						href = a.Val
					case "hreflang":
						hreflang = a.Val
					}
				}
				if rel == "canonical" && hm.Canonical == "" {
					hm.Canonical = strings.TrimSpace(href)
				}
				if strings.Contains(rel, "alternate") && hreflang != "" {
					hl := strings.ToLower(strings.TrimSpace(hreflang))
					if !seenLang[hl] {
						seenLang[hl] = true
						hm.Hreflang = append(hm.Hreflang, hl)
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return hm
}
