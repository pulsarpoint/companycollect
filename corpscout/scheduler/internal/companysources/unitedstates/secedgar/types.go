package secedgar

import "encoding/json"

const (
	SourceKey    = "secedgar"
	SourceSlug   = "united_states_sec_edgar"
	SourceName   = "SEC EDGAR company tickers"
	countryISO2  = "US"
	databaseName = "corpscout_sources"
)

type CompanyTickerRecord struct {
	SourceKey   string          `json:"source_key"`
	SourceIndex int             `json:"source_index"`
	CIK         int             `json:"cik"`
	CIKString   string          `json:"cik_string"`
	CIK10       string          `json:"cik10"`
	Ticker      string          `json:"ticker"`
	Title       string          `json:"title"`
	RawPayload  json.RawMessage `json:"-"`
	PayloadHash string          `json:"-"`
}

type companyTickerPayload struct {
	CIK    int    `json:"cik_str"`
	Ticker string `json:"ticker"`
	Title  string `json:"title"`
}
