package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func TestListNACECodesReturnsRootChildrenWhenParentIDIsOmitted(t *testing.T) {
	q := &stubQuerier{}
	classificationID := uuid.New()
	sectionID := uuid.New()
	q.On("ListNACECodeChildren", mock.Anything, db.ListNACECodeChildrenParams{
		Revision: "2.1",
	}).Return([]db.ListNACECodeChildrenRow{{
		ID:               sectionID,
		ClassificationID: classificationID,
		Code:             "A",
		NormalizedCode:   "A",
		Level:            1,
		LevelName:        "section",
		Title:            "Agriculture, forestry and fishing",
		Active:           true,
		HasChildren:      true,
	}}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/nace/codes?revision=2.1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string][]map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body["items"], 1)
	require.Equal(t, sectionID.String(), body["items"][0]["id"])
	require.Equal(t, "A", body["items"][0]["code"])
	require.Equal(t, "section", body["items"][0]["level_name"])
	require.Equal(t, true, body["items"][0]["has_children"])
	q.AssertExpectations(t)
}

func TestListNACECodesPassesParentIDForChildDrilldown(t *testing.T) {
	q := &stubQuerier{}
	parentID := uuid.New()
	q.On("ListNACECodeChildren", mock.Anything, db.ListNACECodeChildrenParams{
		Revision: "2.1",
		ParentID: pgtype.UUID{Bytes: parentID, Valid: true},
	}).Return([]db.ListNACECodeChildrenRow{}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/nace/codes?revision=2.1&parent_id="+parentID.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	q.AssertExpectations(t)
}

func TestListNACECodesRejectsInvalidParentID(t *testing.T) {
	q := &stubQuerier{}
	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/nace/codes?parent_id=not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), `"error":"parent_id must be a valid UUID"`)
}

func TestListNACERevisionsReturnsTaxonomyStates(t *testing.T) {
	q := &stubQuerier{}
	q.On("ListNACETaxonomyState", mock.Anything).Return([]db.VNaceTaxonomyState{{
		Revision:    "2.1",
		Name:        "NACE Rev. 2.1",
		ActiveCodes: 1047,
		Sections:    22,
		Divisions:   87,
		Groups:      287,
		Classes:     651,
	}}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/nace/revisions", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"revision":"2.1"`)
	require.Contains(t, w.Body.String(), `"sections":22`)
	q.AssertExpectations(t)
}

func (s *stubQuerier) ListNACECodeChildren(
	ctx context.Context,
	arg db.ListNACECodeChildrenParams,
) ([]db.ListNACECodeChildrenRow, error) {
	ret := s.Called(ctx, arg)
	if v, ok := ret.Get(0).([]db.ListNACECodeChildrenRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) ListNACETaxonomyState(ctx context.Context) ([]db.VNaceTaxonomyState, error) {
	ret := s.Called(ctx)
	if v, ok := ret.Get(0).([]db.VNaceTaxonomyState); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}
