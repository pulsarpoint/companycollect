package secedgar

import "encoding/json"

const (
	SourceKey          = "secedgar"
	SourceSlug         = "united_states_sec_edgar"
	SourceName         = "SEC EDGAR company tickers"
	DefaultDownloadURL = "https://www.sec.gov/files/company_tickers.json"

	// DefaultUserAgent is sent to SEC EDGAR when no override is configured.
	// SEC's fair-access policy rejects generic User-Agents with HTTP 403 and
	// requires a descriptive identifier that includes a contact email.
	// See https://www.sec.gov/os/webmaster-faq#developers
	DefaultUserAgent = "CorpScout CountryData/1.0 (+https://pulsarpoint.com; goran.raovic@gmail.com)"
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
