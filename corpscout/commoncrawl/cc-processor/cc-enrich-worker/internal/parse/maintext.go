package parse

import (
	"bytes"
	nurl "net/url"
	"strings"

	"github.com/markusmobius/go-trafilatura"
)

// extractMainContent is the trafilatura entry point, swappable in tests so the fallback
// branch can be driven deterministically.
var extractMainContent = trafilatura.Extract

// MainText extracts the main readable content of a page for embedding — navigation, cookie
// banners and footer boilerplate excluded — so the vector represents what the company says,
// not the page chrome. Whenever trafilatura errors, panics, or finds nothing, it falls back
// to the whole-document text walk, so a failed extraction never yields less than the walk's
// text and a pathological page can never crash the worker.
func MainText(body []byte, pageURL string) string {
	if t := trafilaturaText(body, pageURL); t != "" {
		return t
	}
	text, _, _ := ParseHTML(string(body))
	return text
}

// trafilaturaText isolates the third-party extraction; a panic from the dependency chain on
// pathological crawl HTML is contained here and reported as "no result" (walk fallback).
func trafilaturaText(body []byte, pageURL string) (text string) {
	defer func() {
		if r := recover(); r != nil {
			text = ""
		}
	}()
	opts := trafilatura.Options{ExcludeComments: true}
	if u, err := nurl.Parse(pageURL); err == nil {
		opts.OriginalURL = u
	}
	res, err := extractMainContent(bytes.NewReader(body), opts)
	if err != nil || res == nil {
		return ""
	}
	return strings.TrimSpace(res.ContentText)
}
