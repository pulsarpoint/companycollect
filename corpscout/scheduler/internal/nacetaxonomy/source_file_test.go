package nacetaxonomy

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownloadSourceFileComputesSHA256AndMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rdf+xml")
		w.Header().Set("ETag", "etag-1")
		w.Header().Set("Last-Modified", "Tue, 02 Jun 2026 09:00:00 GMT")
		_, _ = w.Write([]byte("nace fixture"))
	}))
	defer server.Close()

	file, err := DownloadSourceFile(t.Context(), server.Client(), server.URL, 1_000_000)

	require.NoError(t, err)
	require.Equal(t, "cdfc91f61b035ddf5809186f3198337db5d45430dc70b058ad32dab1727b1480", file.SHA256)
	require.Equal(t, int64(12), file.ContentLengthBytes)
	require.Equal(t, "application/rdf+xml", file.ContentType)
	require.Equal(t, "etag-1", file.ETag)
	require.Equal(t, "Tue, 02 Jun 2026 09:00:00 GMT", file.LastModified)
	require.Equal(t, []byte("nace fixture"), file.Body)
}

func TestDownloadSourceFileRejectsTooLargeResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("too-large"))
	}))
	defer server.Close()

	_, err := DownloadSourceFile(t.Context(), server.Client(), server.URL, 3)

	require.ErrorContains(t, err, "nace source file exceeds maximum size")
}

func TestDownloadSourceFileRejectsHTTPErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	defer server.Close()

	_, err := DownloadSourceFile(t.Context(), server.Client(), server.URL, 1_000_000)

	require.ErrorContains(t, err, "download nace source file failed with status 502")
}
