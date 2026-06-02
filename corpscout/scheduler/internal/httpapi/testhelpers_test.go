package httpapi_test

import (
	"context"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
)

// stubQuerier implements db.Querier for use in handler tests.
// Methods called by tests use mock.Called; all others return zero values.
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

// --- methods used by test cases (use mock.Called) ---

func (s *stubQuerier) GetCompany(ctx context.Context, id uuid.UUID) (db.Company, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(db.Company), ret.Error(1)
}

func (s *stubQuerier) ListCountries(ctx context.Context) ([]db.Country, error) {
	ret := s.Called(ctx)
	if v, ok := ret.Get(0).([]db.Country); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

// --- stub-only methods (zero return values, no mock.On needed) ---

func (s *stubQuerier) CountDomains(ctx context.Context, arg db.CountDomainsParams) (int64, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(int64), ret.Error(1)
}

func (s *stubQuerier) CountBrregSourceEntries(ctx context.Context, arg db.CountBrregSourceEntriesParams) (int64, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(int64), ret.Error(1)
}

func (s *stubQuerier) GetSourceByName(ctx context.Context, name string) (db.DataSource, error) {
	ret := s.Called(ctx, name)
	return ret.Get(0).(db.DataSource), ret.Error(1)
}

func (s *stubQuerier) GetStats(ctx context.Context) (db.GetStatsRow, error) {
	ret := s.Called(ctx)
	return ret.Get(0).(db.GetStatsRow), ret.Error(1)
}

func (s *stubQuerier) ListDomains(ctx context.Context, arg db.ListDomainsParams) ([]db.ListDomainsRow, error) {
	ret := s.Called(ctx, arg)
	if v, ok := ret.Get(0).([]db.ListDomainsRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) ListSources(ctx context.Context) ([]db.DataSource, error) {
	ret := s.Called(ctx)
	if v, ok := ret.Get(0).([]db.DataSource); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) ListBrregSourceEntries(ctx context.Context, arg db.ListBrregSourceEntriesParams) ([]db.ListBrregSourceEntriesRow, error) {
	ret := s.Called(ctx, arg)
	if v, ok := ret.Get(0).([]db.ListBrregSourceEntriesRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) UpdateSourceEnabled(ctx context.Context, arg db.UpdateSourceEnabledParams) error {
	ret := s.Called(ctx, arg)
	return ret.Error(0)
}

func (s *stubQuerier) UpdateSourceSchedule(ctx context.Context, arg db.UpdateSourceScheduleParams) error {
	ret := s.Called(ctx, arg)
	return ret.Error(0)
}

func (s *stubQuerier) UpdateSourceScheduleEnabled(ctx context.Context, arg db.UpdateSourceScheduleEnabledParams) error {
	if !s.hasExpectation("UpdateSourceScheduleEnabled") {
		return nil
	}
	ret := s.Called(ctx, arg)
	return ret.Error(0)
}

func (s *stubQuerier) UpdateSourceConfig(ctx context.Context, arg db.UpdateSourceConfigParams) error {
	return nil
}

func (s *stubQuerier) UpdateSourceStarted(ctx context.Context, name string) error {
	return nil
}

func (s *stubQuerier) UpsertCompanyDomain(ctx context.Context, arg db.UpsertCompanyDomainParams) (db.CompanyDomain, error) {
	return db.CompanyDomain{}, nil
}

func (s *stubQuerier) UpdateCompanyEnrichment(ctx context.Context, arg db.UpdateCompanyEnrichmentParams) (db.Company, error) {
	return db.Company{}, nil
}
func (s *stubQuerier) UpsertCompanyEmail(ctx context.Context, arg db.UpsertCompanyEmailParams) (db.CompanyEmail, error) {
	return db.CompanyEmail{}, nil
}
func (s *stubQuerier) UpsertCompanyIndustry(ctx context.Context, arg db.UpsertCompanyIndustryParams) (db.CompanyIndustry, error) {
	return db.CompanyIndustry{}, nil
}
func (s *stubQuerier) UpsertCompanyLocation(ctx context.Context, arg db.UpsertCompanyLocationParams) (db.CompanyLocation, error) {
	return db.CompanyLocation{}, nil
}
func (s *stubQuerier) UpsertCompanyMarket(ctx context.Context, arg db.UpsertCompanyMarketParams) (db.CompanyMarket, error) {
	return db.CompanyMarket{}, nil
}
func (s *stubQuerier) UpsertCompanyPhone(ctx context.Context, arg db.UpsertCompanyPhoneParams) (db.CompanyPhone, error) {
	return db.CompanyPhone{}, nil
}
func (s *stubQuerier) UpsertCompanyService(ctx context.Context, arg db.UpsertCompanyServiceParams) (db.CompanyService, error) {
	return db.CompanyService{}, nil
}

func (s *stubQuerier) UpsertDomain(ctx context.Context, domain string) (db.Domain, error) {
	return db.Domain{}, nil
}

func (s *stubQuerier) UpsertCompanyRelationship(ctx context.Context, arg db.UpsertCompanyRelationshipParams) (db.CompanyRelationship, error) {
	return db.CompanyRelationship{}, nil
}
func (s *stubQuerier) GetCompanyBySlug(ctx context.Context, canonicalSlug string) (db.Company, error) {
	ret := s.Called(ctx, canonicalSlug)
	return ret.Get(0).(db.Company), ret.Error(1)
}
func (s *stubQuerier) CountCompanySuggestionReviews(ctx context.Context, arg db.CountCompanySuggestionReviewsParams) (int32, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(int32), ret.Error(1)
}
func (s *stubQuerier) CountSuggestionReviewItemStatuses(ctx context.Context, suggestionID uuid.UUID) (db.CountSuggestionReviewItemStatusesRow, error) {
	return db.CountSuggestionReviewItemStatusesRow{}, nil
}
func (s *stubQuerier) GetSuggestionByID(ctx context.Context, id uuid.UUID) (db.Suggestion, error) {
	if !s.hasExpectation("GetSuggestionByID") {
		return db.Suggestion{}, nil
	}
	ret := s.Called(ctx, id)
	return ret.Get(0).(db.Suggestion), ret.Error(1)
}
func (s *stubQuerier) InsertSuggestion(ctx context.Context, arg db.InsertSuggestionParams) (db.Suggestion, error) {
	return db.Suggestion{}, nil
}
func (s *stubQuerier) InsertSuggestionCompanyFinancial(ctx context.Context, arg db.InsertSuggestionCompanyFinancialParams) (db.SuggestionCompanyFinancial, error) {
	return db.SuggestionCompanyFinancial{}, nil
}
func (s *stubQuerier) ListCompanySuggestionReviewIDs(ctx context.Context) ([]uuid.UUID, error) {
	return nil, nil
}
func (s *stubQuerier) ListCompanySuggestionReviews(ctx context.Context, arg db.ListCompanySuggestionReviewsParams) ([]db.ListCompanySuggestionReviewsRow, error) {
	ret := s.Called(ctx, arg)
	if v, ok := ret.Get(0).([]db.ListCompanySuggestionReviewsRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}
func (s *stubQuerier) ListPendingCompanySuggestionReviewItems(ctx context.Context, suggestionID uuid.UUID) ([]db.ListPendingCompanySuggestionReviewItemsRow, error) {
	if !s.hasExpectation("ListPendingCompanySuggestionReviewItems") {
		return nil, nil
	}
	ret := s.Called(ctx, suggestionID)
	if v, ok := ret.Get(0).([]db.ListPendingCompanySuggestionReviewItemsRow); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}
func (s *stubQuerier) GetSuggestionCompanyDomainByID(ctx context.Context, id uuid.UUID) (db.SuggestionCompanyDomain, error) {
	return db.SuggestionCompanyDomain{}, nil
}
func (s *stubQuerier) GetSuggestionCompanyEmailByID(ctx context.Context, id uuid.UUID) (db.SuggestionCompanyEmail, error) {
	return db.SuggestionCompanyEmail{}, nil
}
func (s *stubQuerier) GetSuggestionCompanyFinancialByID(ctx context.Context, id uuid.UUID) (db.SuggestionCompanyFinancial, error) {
	return db.SuggestionCompanyFinancial{}, nil
}
func (s *stubQuerier) GetSuggestionCompanyIndustryByID(ctx context.Context, id uuid.UUID) (db.SuggestionCompanyIndustry, error) {
	return db.SuggestionCompanyIndustry{}, nil
}
func (s *stubQuerier) GetSuggestionCompanyLocationByID(ctx context.Context, id uuid.UUID) (db.SuggestionCompanyLocation, error) {
	return db.SuggestionCompanyLocation{}, nil
}
func (s *stubQuerier) GetSuggestionCompanyMarketByID(ctx context.Context, id uuid.UUID) (db.SuggestionCompanyMarket, error) {
	return db.SuggestionCompanyMarket{}, nil
}
func (s *stubQuerier) GetSuggestionCompanyPhoneByID(ctx context.Context, id uuid.UUID) (db.SuggestionCompanyPhone, error) {
	return db.SuggestionCompanyPhone{}, nil
}
func (s *stubQuerier) GetSuggestionCompanyProfileByID(ctx context.Context, id uuid.UUID) (db.SuggestionCompanyProfile, error) {
	return db.SuggestionCompanyProfile{}, nil
}
func (s *stubQuerier) GetSuggestionCompanyRelationshipByID(ctx context.Context, id uuid.UUID) (db.SuggestionCompanyRelationship, error) {
	return db.SuggestionCompanyRelationship{}, nil
}
func (s *stubQuerier) GetSuggestionCompanyServiceByID(ctx context.Context, id uuid.UUID) (db.SuggestionCompanyService, error) {
	return db.SuggestionCompanyService{}, nil
}
func (s *stubQuerier) MarkSuggestionCompanyDomainApplied(ctx context.Context, arg db.MarkSuggestionCompanyDomainAppliedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyDomainRejected(ctx context.Context, arg db.MarkSuggestionCompanyDomainRejectedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyEmailApplied(ctx context.Context, arg db.MarkSuggestionCompanyEmailAppliedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyEmailRejected(ctx context.Context, arg db.MarkSuggestionCompanyEmailRejectedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyFinancialApplied(ctx context.Context, arg db.MarkSuggestionCompanyFinancialAppliedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyFinancialRejected(ctx context.Context, arg db.MarkSuggestionCompanyFinancialRejectedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyIndustryApplied(ctx context.Context, arg db.MarkSuggestionCompanyIndustryAppliedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyIndustryRejected(ctx context.Context, arg db.MarkSuggestionCompanyIndustryRejectedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyLocationApplied(ctx context.Context, arg db.MarkSuggestionCompanyLocationAppliedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyLocationRejected(ctx context.Context, arg db.MarkSuggestionCompanyLocationRejectedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyMarketApplied(ctx context.Context, arg db.MarkSuggestionCompanyMarketAppliedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyMarketRejected(ctx context.Context, arg db.MarkSuggestionCompanyMarketRejectedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyPhoneApplied(ctx context.Context, arg db.MarkSuggestionCompanyPhoneAppliedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyPhoneRejected(ctx context.Context, arg db.MarkSuggestionCompanyPhoneRejectedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyProfileApplied(ctx context.Context, arg db.MarkSuggestionCompanyProfileAppliedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyProfileRejected(ctx context.Context, arg db.MarkSuggestionCompanyProfileRejectedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyRelationshipApplied(ctx context.Context, arg db.MarkSuggestionCompanyRelationshipAppliedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyRelationshipRejected(ctx context.Context, arg db.MarkSuggestionCompanyRelationshipRejectedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyServiceApplied(ctx context.Context, arg db.MarkSuggestionCompanyServiceAppliedParams) error {
	return nil
}
func (s *stubQuerier) MarkSuggestionCompanyServiceRejected(ctx context.Context, arg db.MarkSuggestionCompanyServiceRejectedParams) error {
	return nil
}
func (s *stubQuerier) UpdateSuggestionAggregateStatus(ctx context.Context, arg db.UpdateSuggestionAggregateStatusParams) error {
	return nil
}
func (s *stubQuerier) UpdateSuggestionCreatedCompany(ctx context.Context, arg db.UpdateSuggestionCreatedCompanyParams) error {
	return nil
}

func (s *stubQuerier) InsertCompany(ctx context.Context, arg db.InsertCompanyParams) (db.Company, error) {
	return db.Company{}, nil
}

// --- helpers ---

// newTestHandlers creates a Handlers instance with the given stub, nil river client and nil pool.
func newTestHandlers(q db.Querier) *httpapi.Handlers {
	return httpapi.NewHandlers(q, nil, nil, nil, "", nil, "")
}

func routerFor(h *httpapi.Handlers) chi.Router {
	r := chi.NewRouter()
	h.RegisterRoutes(r)
	return r
}

func (s *stubQuerier) ReviewCompanyDomain(ctx context.Context, arg db.ReviewCompanyDomainParams) error {
	if !s.hasExpectation("ReviewCompanyDomain") {
		return nil
	}
	ret := s.Called(ctx, arg)
	return ret.Error(0)
}

// Domain crawl job stubs (new in recent sqlc-generate)
func (s *stubQuerier) GetDomainByID(ctx context.Context, id uuid.UUID) (db.GetDomainByIDRow, error) {
	return db.GetDomainByIDRow{}, nil
}

// --- domain import stubs ---

func (s *stubQuerier) GetCompanyByExactName(ctx context.Context, lower string) (db.Company, error) {
	if !s.hasExpectation("GetCompanyByExactName") {
		return db.Company{}, nil
	}
	ret := s.Called(ctx, lower)
	return ret.Get(0).(db.Company), ret.Error(1)
}

func (s *stubQuerier) InsertImportBatch(ctx context.Context, arg db.InsertImportBatchParams) (db.DomainImportBatch, error) {
	if !s.hasExpectation("InsertImportBatch") {
		return db.DomainImportBatch{}, nil
	}
	ret := s.Called(ctx, arg)
	return ret.Get(0).(db.DomainImportBatch), ret.Error(1)
}

func (s *stubQuerier) UpdateImportBatchRiverJob(ctx context.Context, arg db.UpdateImportBatchRiverJobParams) error {
	return nil
}

func (s *stubQuerier) UpdateImportBatchStarted(ctx context.Context, arg db.UpdateImportBatchStartedParams) error {
	return nil
}

func (s *stubQuerier) UpdateImportBatchCompleted(ctx context.Context, arg db.UpdateImportBatchCompletedParams) error {
	return nil
}

func (s *stubQuerier) UpsertDomainWithSource(ctx context.Context, arg db.UpsertDomainWithSourceParams) (db.Domain, error) {
	if !s.hasExpectation("UpsertDomainWithSource") {
		return db.Domain{ID: uuid.New(), Domain: arg.Domain}, nil
	}
	ret := s.Called(ctx, arg)
	return ret.Get(0).(db.Domain), ret.Error(1)
}

// Financial enrichment / capabilities stubs (Task 9)
func (s *stubQuerier) GetSourcesWithCapabilities(ctx context.Context) ([]db.DataSource, error) {
	return nil, nil
}

func (s *stubQuerier) ListReviewCandidateIDs(ctx context.Context, arg db.ListReviewCandidateIDsParams) ([]uuid.UUID, error) {
	return nil, nil
}

func (s *stubQuerier) UpdateCompanyInfo(ctx context.Context, arg db.UpdateCompanyInfoParams) (db.Company, error) {
	return db.Company{}, nil
}

func (s *stubQuerier) UpdateCompanyRegistryProfile(ctx context.Context, arg db.UpdateCompanyRegistryProfileParams) (db.Company, error) {
	return db.Company{}, nil
}

func (s *stubQuerier) CreateCompanyFinancial(ctx context.Context, arg db.CreateCompanyFinancialParams) (db.CompanyFinancial, error) {
	return db.CompanyFinancial{}, nil
}

func (s *stubQuerier) ApproveCompanyFinancial(ctx context.Context, arg db.ApproveCompanyFinancialParams) error {
	return nil
}

func (s *stubQuerier) RejectCompanyFinancial(ctx context.Context, arg db.RejectCompanyFinancialParams) error {
	return nil
}

func (s *stubQuerier) BulkUpdateCompanyFinancialStatus(ctx context.Context, arg db.BulkUpdateCompanyFinancialStatusParams) error {
	return nil
}

func (s *stubQuerier) CountPendingCompanyFinancials(ctx context.Context) (int32, error) {
	return 0, nil
}

func (s *stubQuerier) ListCompanyFinancials(ctx context.Context, companyID uuid.UUID) ([]db.CompanyFinancial, error) {
	return nil, nil
}

func (s *stubQuerier) ListPendingCompanyFinancialIDs(ctx context.Context) ([]uuid.UUID, error) {
	return nil, nil
}

func (s *stubQuerier) ListPendingCompanyFinancials(ctx context.Context, arg db.ListPendingCompanyFinancialsParams) ([]db.ListPendingCompanyFinancialsRow, error) {
	return nil, nil
}

func (s *stubQuerier) GetBrregWorkflowTranslationAssetState(ctx context.Context) (db.BrregWorkflowVTranslationAssetState, error) {
	ret := s.Called(ctx)
	return ret.Get(0).(db.BrregWorkflowVTranslationAssetState), ret.Error(1)
}

func (s *stubQuerier) GetBrregSourceTranslationAssetState(ctx context.Context) (db.GetBrregSourceTranslationAssetStateRow, error) {
	ret := s.Called(ctx)
	return ret.Get(0).(db.GetBrregSourceTranslationAssetStateRow), ret.Error(1)
}

func (s *stubQuerier) GetBrregWorkflowDomainAssetState(ctx context.Context) (db.BrregWorkflowVDomainAssetState, error) {
	ret := s.Called(ctx)
	return ret.Get(0).(db.BrregWorkflowVDomainAssetState), ret.Error(1)
}

func (s *stubQuerier) GetBrregWorkflowFinancialAssetState(ctx context.Context) (db.BrregWorkflowVFinancialAssetState, error) {
	ret := s.Called(ctx)
	return ret.Get(0).(db.BrregWorkflowVFinancialAssetState), ret.Error(1)
}

func (s *stubQuerier) GetBrregWorkflowEnhancedAssetState(ctx context.Context) (db.BrregWorkflowVEnhancedAssetState, error) {
	ret := s.Called(ctx)
	return ret.Get(0).(db.BrregWorkflowVEnhancedAssetState), ret.Error(1)
}

func (s *stubQuerier) CountBrregWorkflowRawRecords(ctx context.Context, arg db.CountBrregWorkflowRawRecordsParams) (int64, error) {
	ret := s.Called(ctx, arg)
	return ret.Get(0).(int64), ret.Error(1)
}

func (s *stubQuerier) ListBrregWorkflowRawRecords(ctx context.Context, arg db.ListBrregWorkflowRawRecordsParams) ([]db.BrregWorkflowVRawRecordList, error) {
	ret := s.Called(ctx, arg)
	if v, ok := ret.Get(0).([]db.BrregWorkflowVRawRecordList); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) ListBrregWorkflowDomainSearchEvidenceByRawRecord(ctx context.Context, rawRecordID uuid.UUID) ([]db.BrregWorkflowVDomainSearchEvidence, error) {
	ret := s.Called(ctx, rawRecordID)
	if v, ok := ret.Get(0).([]db.BrregWorkflowVDomainSearchEvidence); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (s *stubQuerier) GetBrregWorkflowRawRecordDetail(ctx context.Context, id uuid.UUID) (db.BrregWorkflowVRawRecordDetail, error) {
	ret := s.Called(ctx, id)
	return ret.Get(0).(db.BrregWorkflowVRawRecordDetail), ret.Error(1)
}

// Sync checkpoint stubs
func (s *stubQuerier) GetSyncCheckpoint(ctx context.Context, sourceName string) (db.SourceSyncCheckpoint, error) {
	return db.SourceSyncCheckpoint{}, nil
}

func (s *stubQuerier) InsertCompanyFromRawInput(ctx context.Context, arg db.InsertCompanyFromRawInputParams) (db.Company, error) {
	return db.Company{}, nil
}

// ensure stubQuerier satisfies the interface at compile time
var _ db.Querier = (*stubQuerier)(nil)
