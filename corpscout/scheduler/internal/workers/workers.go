package workers

// DomainImportArgs are the arguments for a CSV domain import River job.
type DomainImportArgs struct {
	BatchID  string `json:"batch_id"`
	CsvS3Key string `json:"csv_s3_key"`
}

func (DomainImportArgs) Kind() string { return "domain_import" }

// EnrichCompanyFinancialsArgs is the job argument for fetching financial data for a company.
type EnrichCompanyFinancialsArgs struct {
	CompanyID  string `json:"company_id"`
	OrgNumber  string `json:"org_number"`
	SourceName string `json:"source_name"`
}

func (EnrichCompanyFinancialsArgs) Kind() string { return "enrich_company_financials" }
