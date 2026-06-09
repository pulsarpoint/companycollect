package countryimport

import (
	"context"
	"net/http"
	"testing"

	"github.com/cockroachdb/errors"
)

func TestSourceErrorKindClassification(t *testing.T) {
	err := WrapSourceError(
		ErrorKindHTTPStatus,
		"finland_prh_ytj_v3",
		"https://example.test",
		"",
		http.StatusNotFound,
		errors.New("not found"),
	)

	if !IsKind(err, ErrorKindHTTPStatus) {
		t.Fatalf("expected IsKind to identify %q", ErrorKindHTTPStatus)
	}

	if kind := Classify(context.DeadlineExceeded); kind != ErrorKindTimeout {
		t.Fatalf("expected deadline exceeded to classify as %q, got %q", ErrorKindTimeout, kind)
	}

	if kind := Classify(errors.Wrap(err, "outer")); kind != ErrorKindHTTPStatus {
		t.Fatalf("expected wrapped source error to classify as %q, got %q", ErrorKindHTTPStatus, kind)
	}
}
