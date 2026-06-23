package main

import (
	"regexp"
	"strings"

	"golang.org/x/net/html"
)

var emailRe = regexp.MustCompile(`[\w.+-]+@[\w-]+\.[\w.-]+`)

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
	emails = dedupe(emailRe.FindAllString(htmlStr, -1))
	return text, emails, socials
}

func extractSocials(hrefs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, h := range hrefs {
		lower := strings.ToLower(h)
		for host, platform := range socialHosts {
			if strings.Contains(lower, host) && !seen[platform] {
				seen[platform] = true
				out = append(out, platform)
			}
		}
	}
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
