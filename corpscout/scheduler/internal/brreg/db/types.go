package brregdb

import (
	"encoding/json"

	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	defaultMaxAttempts     int32 = 5
	jsonPayloadEmptyObject       = "{}"
)

type TaskType string

const (
	TaskTypeTranslate         TaskType = "translate"
	TaskTypeDiscoverDomains   TaskType = "discover_domains"
	TaskTypeConvertFinancials TaskType = "convert_financials"
)

func (t TaskType) String() string {
	return string(t)
}

type ResultStatus string

const (
	ResultStatusSucceeded    ResultStatus = "succeeded"
	ResultStatusFailed       ResultStatus = "failed"
	ResultStatusSkipped      ResultStatus = "skipped"
	ResultStatusPartial      ResultStatus = "partial"
	ResultStatusNotFound     ResultStatus = "not_found"
	ResultStatusNotAvailable ResultStatus = "not_available"
)

func (s ResultStatus) String() string {
	return string(s)
}

type DomainActionType string

const (
	DomainActionExistingWebsiteCheck  DomainActionType = "existing_website_check"
	DomainActionSearchPageFetch       DomainActionType = "search_page_fetch"
	DomainActionSearchResultAnalysis  DomainActionType = "search_result_analysis"
	DomainActionCandidateSiteCrawl    DomainActionType = "candidate_site_crawl"
	DomainActionCandidateSiteAnalysis DomainActionType = "candidate_site_analysis"
	DomainActionDecision              DomainActionType = "domain_decision"
)

func (t DomainActionType) String() string {
	return string(t)
}

type DomainArtifactType string

const (
	DomainArtifactSearchPage       DomainArtifactType = "search_page"
	DomainArtifactSearchCandidates DomainArtifactType = "search_candidates"
	DomainArtifactCrawlPage        DomainArtifactType = "crawl_page"
	DomainArtifactSiteAnalysis     DomainArtifactType = "site_analysis"
	DomainArtifactDecision         DomainArtifactType = "domain_decision"
)

func (t DomainArtifactType) String() string {
	return string(t)
}

type TaskAttemptStatus string

const (
	TaskAttemptStatusSucceeded TaskAttemptStatus = "succeeded"
	TaskAttemptStatusFailed    TaskAttemptStatus = "failed"
	TaskAttemptStatusSkipped   TaskAttemptStatus = "skipped"
)

func (s TaskAttemptStatus) String() string {
	return string(s)
}

type WorkflowRunStatus string

const (
	WorkflowRunStatusRunning   WorkflowRunStatus = "running"
	WorkflowRunStatusSucceeded WorkflowRunStatus = "succeeded"
	WorkflowRunStatusFailed    WorkflowRunStatus = "failed"
	WorkflowRunStatusCancelled WorkflowRunStatus = "cancelled"
)

func (s WorkflowRunStatus) String() string {
	return string(s)
}

type ClaimTaskBatchCommand struct {
	WorkflowRunID    *uuid.UUID
	SelectionHash    string
	BatchSize        int32
	MaxParallelTasks int32
	LeaseSeconds     int32
	MaxAttempts      int32
	WorkerID         *string
	Metadata         json.RawMessage
}

type ClaimCompaniesForTranslationCommand struct {
	Limit            int32
	MaxParallelTasks int32
	LeaseSeconds     int32
	MaxAttempts      int32
	WorkerID         *string
}

type ClaimCompaniesForTranslationResult struct {
	StatusRowsInserted int32
	Companies          []db.ClaimBrregCompanyTranslationBatchRow
}

type ReleaseCompanyTranslationClaimCommand struct {
	CompanyID uuid.UUID
	WorkerID  string
}

type MarkCompanyTranslationStatusCommand struct {
	CompanyID uuid.UUID
	Metadata  json.RawMessage
}

type MarkCompanyTranslationFailedCommand struct {
	CompanyID     uuid.UUID
	Error         string
	ErrorCategory string
	ErrorCode     string
	RetryStrategy string
	MaxAttempts   int32
	Terminal      bool
	Metadata      json.RawMessage
}

type PrepareWorkflowCommand struct {
	Source             string
	Action             string
	TaskType           TaskType
	Trigger            string
	WorkflowID         string
	IDs                []string
	Filters            map[string]string
	Limit              int32
	BatchSize          int32
	MaxAttempts        int32
	DefaultLimit       int32
	DefaultBatchSize   int32
	DefaultMaxAttempts int32
}

type PreparedWorkflow struct {
	WorkflowRunID   uuid.UUID
	SelectionHash   string
	RecordsSelected int32
	BatchSize       int32
	MaxAttempts     int32
}

type IngestRawRecordsResult struct {
	RowsSeen              int32
	RowsWritten           int32
	RowsInsertedNew       int32
	RowsExistingUnchanged int32
	RowsNewVersions       int32
	RawRecordIDs          []uuid.UUID
}

type TaskFailure struct {
	ErrorCategory string
	ErrorCode     string
	RetryStrategy string
}

type SubmitTranslationResultCommand struct {
	Result      db.InsertBrregWorkflowTranslationResultParams
	Failure     *TaskFailure
	MaxAttempts int32
}

type SubmitDomainResultCommand struct {
	Result      db.InsertBrregWorkflowDomainResultParams
	Failure     *TaskFailure
	MaxAttempts int32
}

type SubmitFinancialResultCommand struct {
	Result      db.InsertBrregWorkflowFinancialResultParams
	Failure     *TaskFailure
	MaxAttempts int32
}

type MapRawRecordIndustriesToNACECommand struct {
	RawRecordID  uuid.UUID
	NACERevision string
}

type RecordDomainActionSuccessCommand struct {
	WorkflowRunID   uuid.UUID
	TaskAttemptID   uuid.UUID
	RawRecordID     uuid.UUID
	ActionType      DomainActionType
	Provider        string
	Model           string
	Input           json.RawMessage
	Attempt         int32
	ArtifactType    DomainArtifactType
	ArtifactPayload json.RawMessage
	Metadata        json.RawMessage
}

type RecordDomainActionFailureCommand struct {
	WorkflowRunID uuid.UUID
	TaskAttemptID uuid.UUID
	RawRecordID   uuid.UUID
	ActionType    DomainActionType
	Provider      string
	Model         string
	Input         json.RawMessage
	Attempt       int32
	Error         string
	ErrorCategory string
	ErrorCode     string
	RetryStrategy string
	Metadata      json.RawMessage
}

type FinishWorkflowRunCommand struct {
	WorkflowRunID    uuid.UUID
	Status           WorkflowRunStatus
	RecordsSeen      int32
	RecordsCompleted int32
	RecordsFailed    int32
	MaxAttempts      int32
	Error            *string
}

type RecoverStaleWorkflowRunsCommand struct {
	MinAgeSeconds int32
}

type RecoverStaleWorkflowRunsResult struct {
	WorkflowRunsRecovered int32
	TaskAttemptsRecovered int32
}

type NormalizeSourceProfilesCommand struct {
	IDs     []string
	Filters map[string]string
	Limit   int32
	Trigger string
}

type NormalizeSourceProfilesResult struct {
	RecordsSeen        int32
	CompaniesUpserted  int32
	AddressesUpserted  int32
	IndustriesUpserted int32
	WebsitesUpserted   int32
	DomainsUpserted    int32
	ContactsUpserted   int32
	CapitalUpserted    int32
}

type ConvertSourceCapitalToUSDCommand struct {
	IDs            []string
	Filters        map[string]string
	Limit          int32
	RateDate       string
	ForceReprocess bool
	Trigger        string
}

type ConvertSourceCapitalToUSDResult struct {
	CapitalSeen                    int32
	CapitalConverted               int32
	CapitalSkippedMissingRate      int32
	CapitalSkippedAlreadyConverted int32
	RateDate                       string
}

type EnsurePendingTranslationTermsCommand struct {
	Provider      string
	Model         string
	PromptVersion string
	WorkflowID    string
	Limit         int32
}

type EnsurePendingTranslationTermsResult struct {
	TermsInserted int32
}

type ClaimQueuedTranslationTermsCommand struct {
	Provider      string
	Model         string
	PromptVersion string
	Limit         int32
	MaxAttempts   int32
}

type QueuedTranslationTerm struct {
	ID                   string
	SourceLang           string
	TargetLang           string
	SourceTextNormalized string
	SourceText           string
	TermKey              string
	AttemptCount         int32
}

type TranslationTermResult struct {
	SourceLang           string
	TargetLang           string
	SourceTextNormalized string
	SourceText           string
	TermKey              string
	TranslatedText       string
	Status               string
	Provider             string
	Model                string
	PromptVersion        string
	Error                string
	ErrorCode            string
	Metadata             map[string]any
}

type UpsertTranslationTermsCommand struct {
	Terms []TranslationTermResult
}

type UpsertTranslationTermsResult struct {
	TermsUpserted int32
}

type ApplyCachedTermTranslationsCommand struct {
	PromptVersion string
	Limit         int32
}

type ApplyCachedTermTranslationsResult struct {
	FieldsApplied int32
}

type CountMissingTranslationFieldsResult struct {
	MissingFields int32
}

func jsonObject(value []byte) []byte {
	if len(value) == 0 {
		return []byte(jsonPayloadEmptyObject)
	}
	return []byte(value)
}
