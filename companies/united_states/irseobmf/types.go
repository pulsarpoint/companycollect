package irseobmf

import "encoding/json"

const (
	// SourceKey is the stable public source key used in manifests and CLI output.
	SourceKey = "irseobmf"
	// SourceSlug is the fully-qualified source identity used in error wrapping.
	SourceSlug = "united_states_irs_eo_bmf"
	// SourceName is a human-readable source name.
	SourceName = "IRS Exempt Organizations Business Master File (EO BMF)"

	// DefaultBaseURL is the IRS SOI directory that hosts the EO BMF CSV extracts.
	DefaultBaseURL = "https://www.irs.gov/pub/irs-soi/"
)

// DefaultFiles are the four regional EO BMF CSV files. Concatenated, they give
// national coverage. They are treated as ordered "pages" during download.
var DefaultFiles = []string{"eo1.csv", "eo2.csv", "eo3.csv", "eo4.csv"}

// csvColumns is the exact, ordered EO BMF CSV header (28 columns). Download
// validates the remote header against this list before converting rows.
var csvColumns = []string{
	"EIN", "NAME", "ICO", "STREET", "CITY", "STATE", "ZIP", "GROUP",
	"SUBSECTION", "AFFILIATION", "CLASSIFICATION", "RULING", "DEDUCTIBILITY",
	"FOUNDATION", "ACTIVITY", "ORGANIZATION", "STATUS", "TAX_PERIOD",
	"ASSET_CD", "INCOME_CD", "FILING_REQ_CD", "PF_FILING_REQ_CD", "ACCT_PD",
	"ASSET_AMT", "INCOME_AMT", "REVENUE_AMT", "NTEE_CD", "SORT_NAME",
}

// IrsEoBmfRecord is one source-native EO BMF row. Every field is preserved as a
// string because the source is CSV; financial amounts are parsed to integers
// only at export time. JSON tags match the uppercase CSV column names so the
// NDJSON snapshot round-trips back into this struct.
type IrsEoBmfRecord struct {
	EIN            string `json:"EIN"`
	Name           string `json:"NAME"`
	InCareOf       string `json:"ICO"`
	Street         string `json:"STREET"`
	City           string `json:"CITY"`
	State          string `json:"STATE"`
	Zip            string `json:"ZIP"`
	Group          string `json:"GROUP"`
	Subsection     string `json:"SUBSECTION"`
	Affiliation    string `json:"AFFILIATION"`
	Classification string `json:"CLASSIFICATION"`
	Ruling         string `json:"RULING"`
	Deductibility  string `json:"DEDUCTIBILITY"`
	Foundation     string `json:"FOUNDATION"`
	Activity       string `json:"ACTIVITY"`
	Organization   string `json:"ORGANIZATION"`
	Status         string `json:"STATUS"`
	TaxPeriod      string `json:"TAX_PERIOD"`
	AssetCD        string `json:"ASSET_CD"`
	IncomeCD       string `json:"INCOME_CD"`
	FilingReqCD    string `json:"FILING_REQ_CD"`
	PFFilingReqCD  string `json:"PF_FILING_REQ_CD"`
	AcctPD         string `json:"ACCT_PD"`
	AssetAmt       string `json:"ASSET_AMT"`
	IncomeAmt      string `json:"INCOME_AMT"`
	RevenueAmt     string `json:"REVENUE_AMT"`
	NTEECD         string `json:"NTEE_CD"`
	SortName       string `json:"SORT_NAME"`

	// RawPayload and PayloadHash are populated during process/export from the
	// snapshot line; they are not part of the serialized NDJSON record.
	RawPayload  json.RawMessage `json:"-"`
	PayloadHash string          `json:"-"`
}
