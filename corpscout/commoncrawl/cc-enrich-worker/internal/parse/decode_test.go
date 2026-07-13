package parse

import (
	"strings"
	"testing"
)

func TestDecodeHTMLMetaHTTPEquivLatin1(t *testing.T) {
	body := []byte("<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=iso-8859-1\"></head><body>M\xfcller GmbH</body></html>")
	decoded, name := DecodeHTML(body, "")
	if !strings.Contains(string(decoded), "Müller GmbH") {
		t.Fatalf("decoded = %q, want to contain %q (name=%s)", decoded, "Müller GmbH", name)
	}
}

func TestDecodeHTMLHeaderCharset(t *testing.T) {
	decoded, _ := DecodeHTML([]byte("caf\xe9"), "text/html; charset=iso-8859-1")
	if string(decoded) != "café" {
		t.Fatalf("decoded = %q, want café", decoded)
	}
}

func TestDecodeHTMLUTF8Passthrough(t *testing.T) {
	body := []byte(`<html><head><meta charset="utf-8"></head><body>Müller</body></html>`)
	decoded, name := DecodeHTML(body, "")
	if name != "utf-8" || &decoded[0] != &body[0] {
		t.Fatalf("declared UTF-8 must pass through unchanged (name=%s)", name)
	}
}

func TestDecodeHTMLUndeclaredUTF8NotMangled(t *testing.T) {
	// No declaration anywhere: DetermineEncoding defaults to windows-1252, which would
	// turn every multi-byte rune into mojibake. Valid UTF-8 must always pass through.
	body := []byte("<html><body>Müller — København</body></html>")
	decoded, _ := DecodeHTML(body, "")
	if !strings.Contains(string(decoded), "Müller — København") {
		t.Fatalf("undeclared UTF-8 was mangled: %q", decoded)
	}
}

func TestDecodeHTMLMisdeclaredHeaderKeepsRealUTF8(t *testing.T) {
	// Servers commonly declare latin-1 while serving UTF-8; decoding real UTF-8 as a charmap
	// manufactures mojibake. Valid UTF-8 with multi-byte runes must pass through untouched.
	body := []byte("<html><body>Müller GmbH — København</body></html>")
	decoded, name := DecodeHTML(body, "text/html; charset=iso-8859-1")
	if !strings.Contains(string(decoded), "Müller GmbH — København") {
		t.Fatalf("misdeclared latin-1 header corrupted real UTF-8 (name=%s): %q", name, decoded)
	}
}
