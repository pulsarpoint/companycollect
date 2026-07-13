package security

import (
	"net/http"
	"testing"
)

func TestHeaderMap(t *testing.T) {
	h := http.Header{
		"Content-Security-Policy": {"default-src 'self'"},
		"Server":                  {"nginx/1.18.0"},
		"Set-Cookie":              {"a=1; Path=/", "b=2; HttpOnly"},
		"Vary":                    {"Accept-Encoding", "Cookie"}, // multi-valued non-cookie
	}
	m := HeaderMap(h)
	if m["content-security-policy"] != "default-src 'self'" {
		t.Fatalf("csp: %q", m["content-security-policy"])
	}
	if m["server"] != "nginx/1.18.0" {
		t.Fatalf("server: %q", m["server"])
	}
	if m["set-cookie"] != "a=1; Path=/\nb=2; HttpOnly" {
		t.Fatalf("set-cookie must join with newline: %q", m["set-cookie"])
	}
	if m["vary"] != "Accept-Encoding, Cookie" {
		t.Fatalf("vary must join with comma: %q", m["vary"])
	}
	if _, ok := m["Server"]; ok {
		t.Fatal("keys must be lowercased")
	}
	if HeaderMap(nil) != nil {
		t.Fatal("empty -> nil")
	}
}
