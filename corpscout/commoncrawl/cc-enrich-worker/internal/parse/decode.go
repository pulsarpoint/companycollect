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
// UTF-8 as 1252 would corrupt it, so valid UTF-8 always passes through. A declared (certain)
// charmap encoding is also skipped when the body is valid UTF-8 with a multi-byte rune, since
// misdeclared latin-1-on-UTF-8 pages are common and charmap-decoding real UTF-8 manufactures
// mojibake. Returns the body (decoded or original) and the encoding name that was decided.
func DecodeHTML(body []byte, contentType string) ([]byte, string) {
	enc, name, certain := charset.DetermineEncoding(body, contentType)
	if name == "utf-8" {
		return body, name
	}
	// A body that is valid UTF-8 and contains a multi-byte rune IS UTF-8 no matter what the
	// server declared — misdeclared latin-1 headers on UTF-8 pages are common, and charmap-
	// decoding real UTF-8 manufactures mojibake. The multi-byte requirement protects declared
	// UTF-16: NUL-padded ASCII is "valid UTF-8" but still needs the real decoder. Without any
	// declaration (certain == false) the default is windows-1252, so any valid UTF-8 (including
	// pure ASCII) passes through.
	if utf8.Valid(body) && (!certain || hasNonASCII(body)) {
		return body, "utf-8"
	}
	decoded, _, err := transform.Bytes(enc.NewDecoder(), body)
	if err != nil || len(decoded) == 0 {
		return body, "" // undecodable: keep the bytes, don't label them with an encoding they aren't in
	}
	return decoded, name
}

// hasNonASCII reports whether the body has any byte >= 0x80 (for valid UTF-8, a multi-byte rune).
func hasNonASCII(body []byte) bool {
	for _, b := range body {
		if b >= 0x80 {
			return true
		}
	}
	return false
}
