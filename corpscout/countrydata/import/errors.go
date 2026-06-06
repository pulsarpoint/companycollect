package countryimport

import (
	"context"
	"fmt"
	"net"

	"github.com/cockroachdb/errors"
)

type ErrorKind string

const (
	ErrorKindUnknown       ErrorKind = "unknown"
	ErrorKindNotFound      ErrorKind = "not_found"
	ErrorKindNoSnapshot    ErrorKind = "no_snapshot"
	ErrorKindTimeout       ErrorKind = "timeout"
	ErrorKindHTTPStatus    ErrorKind = "http_status"
	ErrorKindRemoteDecode  ErrorKind = "remote_decode"
	ErrorKindLineDecode    ErrorKind = "line_decode"
	ErrorKindInvalidConfig ErrorKind = "invalid_config"
	ErrorKindFileIO        ErrorKind = "file_io"
	ErrorKindState         ErrorKind = "state"
)

type SourceError struct {
	Kind   ErrorKind
	Source string
	URL    string
	Path   string
	Status int
	Err    error
}

func (e *SourceError) Error() string {
	if e == nil {
		return ""
	}
	message := fmt.Sprintf("%s source error", e.Kind)
	if e.Source != "" {
		message += " for " + e.Source
	}
	if e.URL != "" {
		message += " url=" + e.URL
	}
	if e.Path != "" {
		message += " path=" + e.Path
	}
	if e.Status != 0 {
		message += fmt.Sprintf(" status=%d", e.Status)
	}
	if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	return message
}

func (e *SourceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func WrapSourceError(kind ErrorKind, source string, url string, path string, status int, err error) error {
	return &SourceError{
		Kind:   kind,
		Source: source,
		URL:    url,
		Path:   path,
		Status: status,
		Err:    err,
	}
}

func IsKind(err error, kind ErrorKind) bool {
	return Classify(err) == kind
}

func Classify(err error) ErrorKind {
	if err == nil {
		return ErrorKindUnknown
	}

	var sourceErr *SourceError
	if errors.As(err, &sourceErr) {
		return sourceErr.Kind
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ErrorKindTimeout
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrorKindTimeout
	}

	return ErrorKindUnknown
}
