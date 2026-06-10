package httpapi_test

import (
	"context"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
)

type stubQuerier struct {
	db.Querier
	mock.Mock
}

func (s *stubQuerier) hasExpectation(method string) bool {
	for _, call := range s.ExpectedCalls {
		if call.Method == method {
			return true
		}
	}
	return false
}

func (s *stubQuerier) CountDomains(ctx context.Context, arg db.CountDomainsParams) (int64, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(int64), ret.Error(1)
}

func (s *stubQuerier) CreateSourceActionRun(ctx context.Context, arg db.CreateSourceActionRunParams) (db.DataSourceActionRun, error) {
	ret := s.Called(ctx, arg)
	if fn, ok := ret.Get(0).(func(context.Context, db.CreateSourceActionRunParams) db.DataSourceActionRun); ok {
		return fn(ctx, arg), ret.Error(1)
	}
	return ret.Get(0).(db.DataSourceActionRun), ret.Error(1)
}

func (s *stubQuerier) CreateSourceFileRun(ctx context.Context, arg db.CreateSourceFileRunParams) (db.DataSourceFileRun, error) {
	ret := s.Called(ctx, arg)
	if fn, ok := ret.Get(0).(func(context.Context, db.CreateSourceFileRunParams) db.DataSourceFileRun); ok {
		return fn(ctx, arg), ret.Error(1)
	}
	return ret.Get(0).(db.DataSourceFileRun), ret.Error(1)
}

func (s *stubQuerier) FinishSourceActionRun(ctx context.Context, arg db.FinishSourceActionRunParams) (db.DataSourceActionRun, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(db.DataSourceActionRun), ret.Error(1)
}

func (s *stubQuerier) FinishSourceFileRun(ctx context.Context, arg db.FinishSourceFileRunParams) (db.DataSourceFileRun, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(db.DataSourceFileRun), ret.Error(1)
}

func (s *stubQuerier) GetCompany(ctx context.Context, id uuid.UUID) (db.Company, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(db.Company), ret.Error(1)
}

func (s *stubQuerier) GetCompanyByExactName(ctx context.Context, lower string) (db.Company, error) {
	ret := s.Called(ctx, lower)
	return ret.Get(0).(db.Company), ret.Error(1)
}

func (s *stubQuerier) GetCompanyBySlug(ctx context.Context, canonicalSlug string) (db.Company, error) {
	ret := s.Called(ctx, canonicalSlug)
	return ret.Get(0).(db.Company), ret.Error(1)
}

func (s *stubQuerier) GetDomainByID(ctx context.Context, id uuid.UUID) (db.GetDomainByIDRow, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(db.GetDomainByIDRow), ret.Error(1)
}

func (s *stubQuerier) GetSourceByName(ctx context.Context, name string) (db.GetSourceByNameRow, error) {
	ret := s.Called(ctx, name)
	return ret.Get(0).(db.GetSourceByNameRow), ret.Error(1)
}

func (s *stubQuerier) GetSourceActionByName(ctx context.Context, arg db.GetSourceActionByNameParams) (db.GetSourceActionByNameRow, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(db.GetSourceActionByNameRow), ret.Error(1)
}

func (s *stubQuerier) GetSourceActionRun(ctx context.Context, id uuid.UUID) (db.DataSourceActionRun, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(db.DataSourceActionRun), ret.Error(1)
}

func (s *stubQuerier) GetSourceFileBySourceNameAndKey(ctx context.Context, arg db.GetSourceFileBySourceNameAndKeyParams) (db.GetSourceFileBySourceNameAndKeyRow, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(db.GetSourceFileBySourceNameAndKeyRow), ret.Error(1)
}

func (s *stubQuerier) GetSourceFileRunWithDefinition(ctx context.Context, id uuid.UUID) (db.GetSourceFileRunWithDefinitionRow, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(db.GetSourceFileRunWithDefinitionRow), ret.Error(1)
}

func (s *stubQuerier) GetLatestSuccessfulSourceActionRun(ctx context.Context, arg db.GetLatestSuccessfulSourceActionRunParams) (db.DataSourceActionRun, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(db.DataSourceActionRun), ret.Error(1)
}

func (s *stubQuerier) GetLatestSuccessfulSourceFileRun(ctx context.Context, arg db.GetLatestSuccessfulSourceFileRunParams) (db.GetLatestSuccessfulSourceFileRunRow, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(db.GetLatestSuccessfulSourceFileRunRow), ret.Error(1)
}

func (s *stubQuerier) GetSourcesWithCapabilities(ctx context.Context) ([]db.GetSourcesWithCapabilitiesRow, error) {
	if !s.hasExpectation("GetSourcesWithCapabilities") {
		return nil, nil
	}
	ret := s.Called(ctx)
	if v, ok := ret.Get(0).([]db.GetSourcesWithCapabilitiesRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) GetStats(ctx context.Context) (db.GetStatsRow, error) {
	ret := s.Called(ctx)
	return ret.Get(0).(db.GetStatsRow), ret.Error(1)
}

func (s *stubQuerier) GetSyncCheckpoint(ctx context.Context, sourceName string) (db.SourceSyncCheckpoint, error) {
	if !s.hasExpectation("GetSyncCheckpoint") {
		return db.SourceSyncCheckpoint{}, nil
	}
	ret := s.Called(ctx, sourceName)
	return ret.Get(0).(db.SourceSyncCheckpoint), ret.Error(1)
}

func (s *stubQuerier) InsertCompany(ctx context.Context, arg db.InsertCompanyParams) (db.Company, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(db.Company), ret.Error(1)
}

func (s *stubQuerier) ListCountries(ctx context.Context) ([]db.Country, error) {
	ret := s.Called(ctx)
	if v, ok := ret.Get(0).([]db.Country); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) ListDomains(ctx context.Context, arg db.ListDomainsParams) ([]db.ListDomainsRow, error) {
	ret := s.Called(ctx, arg)
	if v, ok := ret.Get(0).([]db.ListDomainsRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) ListSources(ctx context.Context) ([]db.ListSourcesRow, error) {
	ret := s.Called(ctx)
	if v, ok := ret.Get(0).([]db.ListSourcesRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) ListSourceActionRuns(ctx context.Context, arg db.ListSourceActionRunsParams) ([]db.ListSourceActionRunsRow, error) {
	ret := s.Called(ctx, arg)
	if v, ok := ret.Get(0).([]db.ListSourceActionRunsRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) ListSourceActions(ctx context.Context, name string) ([]db.ListSourceActionsRow, error) {
	ret := s.Called(ctx, name)
	if v, ok := ret.Get(0).([]db.ListSourceActionsRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) ListSourceFileRuns(ctx context.Context, arg db.ListSourceFileRunsParams) ([]db.ListSourceFileRunsRow, error) {
	ret := s.Called(ctx, arg)
	if v, ok := ret.Get(0).([]db.ListSourceFileRunsRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) ListSourceFilesWithLatestRun(ctx context.Context, name string) ([]db.ListSourceFilesWithLatestRunRow, error) {
	ret := s.Called(ctx, name)
	if v, ok := ret.Get(0).([]db.ListSourceFilesWithLatestRunRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) ReviewCompanyDomain(ctx context.Context, arg db.ReviewCompanyDomainParams) error {
	ret := s.Called(ctx, arg)
	return ret.Error(0)
}

func (s *stubQuerier) UpdateSourceActionRunTemporalRunID(ctx context.Context, arg db.UpdateSourceActionRunTemporalRunIDParams) error {
	if !s.hasExpectation("UpdateSourceActionRunTemporalRunID") {
		return nil
	}
	ret := s.Called(ctx, arg)
	return ret.Error(0)
}

func (s *stubQuerier) UpdateSourceFileRunTemporalRunID(ctx context.Context, arg db.UpdateSourceFileRunTemporalRunIDParams) error {
	if !s.hasExpectation("UpdateSourceFileRunTemporalRunID") {
		return nil
	}
	ret := s.Called(ctx, arg)
	return ret.Error(0)
}

func routerFor(h *httpapi.Handlers) chi.Router {
	r := chi.NewRouter()
	h.RegisterRoutes(r)
	return r
}

func routerForHandlers(q db.Querier) chi.Router {
	return routerFor(newTestHandlers(q))
}

func newTestHandlers(q db.Querier) *httpapi.Handlers {
	return httpapi.NewHandlers(q, nil, nil, nil, "", nil, "")
}

var _ db.Querier = (*stubQuerier)(nil)
