package irseobmf

import "encoding/json"

const (
	SourceKey    = "irseobmf"
	SourceSlug   = "united_states_irs_eo_bmf"
	SourceName   = "IRS Exempt Organizations Business Master File (EO BMF)"
	countryISO2  = "US"
	databaseName = "corpscout"
)

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

	RawPayload  json.RawMessage `json:"-"`
	PayloadHash string          `json:"-"`
}
