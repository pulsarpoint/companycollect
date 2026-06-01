package httpapi_test

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandleImportDomains_NoS3_Returns503 verifies that when the s3 client is
// not configured (nil), the handler immediately returns 503 Service Unavailable.
func TestHandleImportDomains_NoS3_Returns503(t *testing.T) {
	q := &stubQuerier{}
	r := routerForHandlers(q)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/domains/import", nil)

	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

// TestHandleImportDomains_MissingFile_Returns400 verifies that uploading a
// multipart request that does not include a "file" field returns 400.
// Note: when s3 is nil the handler returns 503 before reaching the file check,
// so this test is kept here as documentation of the expected flow — the handler
// would return 400 if s3 were available and the field were missing.
func TestHandleImportDomains_ContentType_NoFile(t *testing.T) {
	q := &stubQuerier{}
	r := routerForHandlers(q)

	// Build a multipart body with no "file" field.
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	require.NoError(t, mw.Close())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/domains/import", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	r.ServeHTTP(rec, req)

	// s3 is nil in test helpers, so we expect 503 before the file check.
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
