package models

import "encoding/json"

const SchemaVersion = "brreg-financial-service.lookup.v1"

// LookupRecord is one organization in a batch request.
type LookupRecord struct {
	RecordID             string `json:"record_id"`
	OrganizationNumber   string `json:"organization_number"`
	OrganizationName     string `json:"organization_name,omitempty"`
	LastAnnualReportYear int    `json:"last_annual_report_year,omitempty"`
}

// LookupRequest is the JSON body for POST /v1/brreg/financials/lookup.
type LookupRequest struct {
	Records            []LookupRecord `json:"records"`
	IncludePDFMetadata bool           `json:"include_pdf_metadata"`
	IncludeRawPayload  bool           `json:"include_raw_payload"`
}

// LookupResponse is the JSON body for POST /v1/brreg/financials/lookup response.
type LookupResponse struct {
	SchemaVersion    string         `json:"schema_version"`
	Status           string         `json:"status"`
	RecordsSeen      int            `json:"records_seen"`
	RecordsCompleted int            `json:"records_completed"`
	RecordsFailed    int            `json:"records_failed"`
	DurationMs       int64          `json:"duration_ms"`
	Results          []RecordResult `json:"results"`
}

// RecordResult is the outcome for one organization in the batch.
// Status: succeeded | not_available | unsupported_statement_plan | failed
type RecordResult struct {
	RecordID           string       `json:"record_id"`
	OrganizationNumber string       `json:"organization_number"`
	Status             string       `json:"status"`
	Statements         []Statement  `json:"statements"`
	PDFMetadata        *PDFMetadata `json:"pdf_metadata,omitempty"`
	Warnings           []Warning    `json:"warnings"`
}

// Statement is one normalized BRREG annual-account key-figure record.
// All amount fields are decimal strings (e.g. "72543000000.00") or null.
type Statement struct {
	SourceRecordID                     string            `json:"source_record_id"`
	JournalNumber                      string            `json:"journal_number"`
	FiscalYear                         int               `json:"fiscal_year"`
	PeriodStart                        string            `json:"period_start"`
	PeriodEnd                          string            `json:"period_end"`
	StatementType                      string            `json:"statement_type"`
	OriginalCurrency                   string            `json:"original_currency"`
	RevenueOriginalAmount              *string           `json:"revenue_original_amount"`
	SalesRevenueOriginalAmount         *string           `json:"sales_revenue_original_amount"`
	OperatingProfitOriginalAmount      *string           `json:"operating_profit_original_amount"`
	ProfitBeforeTaxOriginalAmount      *string           `json:"profit_before_tax_original_amount"`
	TaxExpenseOriginalAmount           *string           `json:"tax_expense_original_amount"`
	NetIncomeOriginalAmount            *string           `json:"net_income_original_amount"`
	TotalResultOriginalAmount          *string           `json:"total_result_original_amount"`
	TotalAssetsOriginalAmount          *string           `json:"total_assets_original_amount"`
	CurrentAssetsOriginalAmount        *string           `json:"current_assets_original_amount"`
	FixedAssetsOriginalAmount          *string           `json:"fixed_assets_original_amount"`
	TotalEquityOriginalAmount          *string           `json:"total_equity_original_amount"`
	TotalLiabilitiesOriginalAmount     *string           `json:"total_liabilities_original_amount"`
	ShortTermLiabilitiesOriginalAmount *string           `json:"short_term_liabilities_original_amount"`
	LongTermLiabilitiesOriginalAmount  *string           `json:"long_term_liabilities_original_amount"`
	Facts                              map[string]string `json:"facts,omitempty"`
	Metadata                           StatementMetadata `json:"metadata"`
	Evidence                           StatementEvidence `json:"evidence"`
	RawPayload                         json.RawMessage   `json:"raw_payload,omitempty"`
}

// StatementMetadata contains descriptive metadata about the statement.
type StatementMetadata struct {
	OrganizationForm    string `json:"organization_form"`
	IsParentCompany     bool   `json:"is_parent_company"`
	StatementPlan       string `json:"statement_plan"`
	AccountingRules     string `json:"accounting_rules"`
	SmallCompany        bool   `json:"small_company"`
	NotAudited          bool   `json:"not_audited"`
	AuditOptOut         bool   `json:"audit_opt_out"`
	LiquidationAccounts bool   `json:"liquidation_accounts"`
}

// StatementEvidence records where data came from and how to verify it.
type StatementEvidence struct {
	Source         string `json:"source"`
	SourceURL      string `json:"source_url"`
	DetailURL      string `json:"detail_url"`
	RawPayloadHash string `json:"raw_payload_hash"`
}

// PDFMetadata describes available annual-account PDFs.
type PDFMetadata struct {
	AvailableYears      []string `json:"available_years"`
	DownloadURLTemplate string   `json:"download_url_template"`
}

// Warning is a non-fatal diagnostic for a record result.
type Warning struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Detail  map[string]any `json:"detail,omitempty"`
}
