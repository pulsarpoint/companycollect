package httpapi

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"
)

// newPostgRESTProxy builds a reverse proxy that forwards /api/v1/db/* to PostgREST.
// The /api/v1/db prefix is stripped before forwarding.
func newPostgRESTProxy(postgrestURL string) (http.HandlerFunc, error) {
	target, err := url.Parse(postgrestURL)
	if err != nil {
		return nil, errors.Wrap(err, "parse postgrest URL")
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return nil, errors.New("postgrest URL must use http or https")
	}
	if target.Host == "" {
		return nil, errors.New("postgrest URL must include a host")
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			// Strip /api/v1/db prefix, keep the rest (table + query params)
			req.URL.Path = strings.TrimPrefix(req.URL.Path, "/api/v1/db")
			if req.URL.Path == "" {
				req.URL.Path = "/"
			}
			req.Host = target.Host
		},
	}

	return proxy.ServeHTTP, nil
}
