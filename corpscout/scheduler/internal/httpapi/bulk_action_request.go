package httpapi

import (
	"encoding/json"
	"net/http"
)

const (
	approveRejectBulkActionMessage = "action must be approve or reject"
	reviewBulkActionMessage        = "action must be approved, rejected, or superseded"
)

var (
	approveRejectBulkActions = map[string]struct{}{
		"approve": {},
		"reject":  {},
	}
	reviewBulkActions = map[string]struct{}{
		"approved":   {},
		"rejected":   {},
		"superseded": {},
	}
)

type bulkActionRequest struct {
	IDs    []string `json:"ids"`
	Action string   `json:"action"`
}

type actionRequest struct {
	Action string `json:"action"`
}

func decodeActionRequest(r *http.Request, validActions map[string]struct{}, invalidActionMessage string) (string, error) {
	var req actionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return "", requestError("invalid request body")
	}
	if _, ok := validActions[req.Action]; !ok {
		return "", requestError(invalidActionMessage)
	}
	return req.Action, nil
}

func decodeBulkActionRequest(r *http.Request, validActions map[string]struct{}, invalidActionMessage string) (bulkActionRequest, error) {
	var req bulkActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return req, requestError("invalid request body")
	}
	if len(req.IDs) == 0 {
		return req, requestError("ids must not be empty")
	}
	if _, ok := validActions[req.Action]; !ok {
		return req, requestError(invalidActionMessage)
	}
	return req, nil
}

type requestError string

func (e requestError) Error() string {
	return string(e)
}
