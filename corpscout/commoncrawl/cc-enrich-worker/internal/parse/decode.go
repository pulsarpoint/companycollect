package parse

import (
	"unicode/utf8"

	"golang.org/x/net/html/charset"
	"golang.org/x/text/transform"
)

// DecodeHTML transcodes an HTML body to UTF-8 using the encoding declared in the Content-Type
// header, BOM, <meta charset> or <meta http-equiv> form (charset.DetermineEncoding sniffs all).
// Already-UTF-8 bodies are returned unchanged (no copy). Undeclared encodings default to
// windows-1252 per the HTML spec, EXCEPT when the body is valid UTF-8 — reinterpreting real
// UTF-8 as 1252 would corrupt it, so valid UTF-8 always passes through. Returns the body
// (decoded or original) and the encoding name that was decided.
func DecodeHTML(body []byte, contentType string) ([]byte, string) {
	enc, name, certain := charset.DetermineEncoding(body, contentType)
	if name == "utf-8" {
		return body, name
	}
	if !certain && utf8.Valid(body) {
		return body, "utf-8"
	}
	decoded, _, err := transform.Bytes(enc.NewDecoder(), body)
	if err != nil || len(decoded) == 0 {
		return body, name
	}
	return decoded, name
}
