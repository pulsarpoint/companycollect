package parse

import (
	"net/url"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/net/html"
)

// emailRe requires an alphabetic TLD-shaped final label so version strings (bootstrap@5.3.3) and
// trailing sentence punctuation (info@acme.com.) don't match; assetExt then rejects the
// email-shaped filenames that survive it (logo@2x.png).
var emailRe = regexp.MustCompile(`[\w.+-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,24}`)

// assetExt lists file extensions that produce email-shaped tokens in raw markup, e.g. retina
// image names (logo@2x.png) and scoped-package CDN paths; a match ending in one of these is a
// filename, not an address.
var assetExt = map[string]bool{
	"png": true, "jpg": true, "jpeg": true, "gif": true, "svg": true, "webp": true,
	"avif": true, "ico": true, "bmp": true, "css": true, "js": true, "mjs": true,
	"json": true, "map": true, "woff": true, "ttf": true, "otf": true, "eot": true,
	"mp4": true, "webm": true, "mov": true, "mp3": true, "wav": true, "pdf": true,
	"zip": true, "gz": true, "html": true, "htm": true, "php": true,
}

func findEmails(s string) []string {
	var out []string
	for _, m := range emailRe.FindAllString(s, -1) {
		ext := strings.ToLower(m[strings.LastIndexByte(m, '.')+1:])
		if assetExt[ext] {
			continue
		}
		out = append(out, m)
	}
	return dedupe(out)
}

// socialHosts maps a host substring to the platform label.
var socialHosts = map[string]string{
	"facebook.com":  "facebook",
	"fb.com":        "facebook",
	"twitter.com":   "twitter",
	"x.com":         "twitter",
	"linkedin.com":  "linkedin",
	"instagram.com": "instagram",
	"youtube.com":   "youtube",
	"youtu.be":      "youtube",
	"tiktok.com":    "tiktok",
	"pinterest.com": "pinterest",
	"github.com":    "github",
}

// ParseHTML extracts visible text, emails, and social-platform links from an HTML page.
func ParseHTML(htmlStr string) (text string, emails []string, socials []string) {
	doc, err := html.Parse(strings.NewReader(htmlStr))
	if err != nil {
		text = htmlStr
	} else {
		var sb strings.Builder
		var hrefs []string
		var walk func(*html.Node)
		walk = func(n *html.Node) {
			if n.Type == html.ElementNode {
				if n.Data == "script" || n.Data == "style" {
					return // don't descend into non-visible content
				}
				if n.Data == "a" {
					for _, a := range n.Attr {
						if a.Key == "href" {
							hrefs = append(hrefs, a.Val)
						}
					}
				}
			}
			if n.Type == html.TextNode {
				if t := strings.TrimSpace(n.Data); t != "" {
					sb.WriteString(t)
					sb.WriteByte(' ')
				}
			}
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				walk(c)
			}
		}
		walk(doc)
		text = strings.TrimSpace(sb.String())
		socials = extractSocials(hrefs)
	}
	emails = findEmails(htmlStr)
	return text, emails, socials
}

// Emails returns the de-duplicated email addresses in s via the same regex as ParseHTML, but
// without the full HTML→text conversion — cheap enough to run on every page in the tech pass.
func Emails(s string) []string { return findEmails(s) }

// extractSocials matches each href's parsed hostname exactly (or as a dot-suffix) against the
// known platform hosts — substring matching would tag wix.com/netflix.com links as "x.com".
// The result is sorted so identical inputs always produce the same row.
func extractSocials(hrefs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, h := range hrefs {
		u, err := url.Parse(strings.TrimSpace(h))
		if err != nil {
			continue
		}
		host := strings.ToLower(u.Hostname())
		if host == "" {
			continue
		}
		for cand, platform := range socialHosts {
			if (host == cand || strings.HasSuffix(host, "."+cand)) && !seen[platform] {
				seen[platform] = true
				out = append(out, platform)
			}
		}
	}
	sort.Strings(out)
	return out
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
