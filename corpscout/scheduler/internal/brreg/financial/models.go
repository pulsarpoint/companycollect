package financial

import "encoding/json"

const (
	DefaultBaseURL = "https://data.brreg.no"

	StatusSucceeded                = "succeeded"
	StatusNotAvailable             = "not_available"
	StatusUnsupportedStatementPlan = "unsupported_statement_plan"
	StatusFailed                   = "failed"
)

type LookupRecord struct {
	RecordID           string
	OrganizationNumber string
	OrganizationName   string
}

type RecordResult struct {
	RecordID           string
	OrganizationNumber string
	Status             string
	Statements         []Statement
	Warnings           []Warning
}

type Statement struct {
	SourceRecordID                    string
	JournalNumber                     string
	FiscalYear                        int32
	PeriodStart                       string
	PeriodEnd                         string
	StatementType                     string
	IsConsolidated                    bool
	OriginalCurrency                  string
	RevenueOriginalAmount             *string
	OperatingIncomeOriginalAmount     *string
	OperatingProfitOriginalAmount     *string
	ProfitBeforeTaxOriginalAmount     *string
	TaxExpenseOriginalAmount          *string
	NetIncomeOriginalAmount           *string
	TotalResultOriginalAmount         *string
	TotalAssetsOriginalAmount         *string
	CurrentAssetsOriginalAmount       *string
	FixedAssetsOriginalAmount         *string
	TotalEquityOriginalAmount         *string
	TotalLiabilitiesOriginalAmount    *string
	CurrentLiabilitiesOriginalAmount  *string
	LongTermLiabilitiesOriginalAmount *string
	Facts                             map[string]string
	Metadata                          StatementMetadata
	Evidence                          StatementEvidence
	RawPayload                        json.RawMessage
}

type StatementMetadata struct {
	OrganizationForm    string `json:"organization_form"`
	IsParentCompany     bool   `json:"is_parent_company"`
	SourceStatementType string `json:"source_statement_type"`
	StatementPlan       string `json:"statement_plan"`
	AccountingRules     string `json:"accounting_rules"`
	SmallCompany        bool   `json:"small_company"`
	NotAudited          bool   `json:"not_audited"`
	AuditOptOut         bool   `json:"audit_opt_out"`
	LiquidationAccounts bool   `json:"liquidation_accounts"`
	JournalNumber       string `json:"journal_number,omitempty"`
	SourceRecordID      string `json:"source_record_id,omitempty"`
}

type StatementEvidence struct {
	Source         string `json:"source"`
	SourceURL      string `json:"source_url"`
	DetailURL      string `json:"detail_url"`
	RawPayloadHash string `json:"raw_payload_hash"`
}

type Warning struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Detail  map[string]any `json:"detail,omitempty"`
}
