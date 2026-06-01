package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeBulkActionRequestValidatesSharedShape(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/bulk", strings.NewReader(`{"ids":["id-1"],"action":"approve"}`))

	parsed, err := decodeBulkActionRequest(req, approveRejectBulkActions, approveRejectBulkActionMessage)

	require.NoError(t, err)
	require.Equal(t, bulkActionRequest{IDs: []string{"id-1"}, Action: "approve"}, parsed)
}

func TestDecodeBulkActionRequestRejectsEmptyIDs(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/bulk", strings.NewReader(`{"ids":[],"action":"approve"}`))

	_, err := decodeBulkActionRequest(req, approveRejectBulkActions, approveRejectBulkActionMessage)

	require.EqualError(t, err, "ids must not be empty")
}

func TestDecodeBulkActionRequestRejectsUnknownAction(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/bulk", strings.NewReader(`{"ids":["id-1"],"action":"bogus"}`))

	_, err := decodeBulkActionRequest(req, approveRejectBulkActions, approveRejectBulkActionMessage)

	require.EqualError(t, err, "action must be approve or reject")
}

func TestDecodeActionRequestValidatesSharedActionShape(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/review/id", strings.NewReader(`{"action":"approved"}`))

	action, err := decodeActionRequest(req, reviewBulkActions, reviewBulkActionMessage)

	require.NoError(t, err)
	require.Equal(t, "approved", action)
}

func TestDecodeActionRequestRejectsUnknownAction(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/review/id", strings.NewReader(`{"action":"bogus"}`))

	_, err := decodeActionRequest(req, reviewBulkActions, reviewBulkActionMessage)

	require.EqualError(t, err, "action must be approved, rejected, or superseded")
}
