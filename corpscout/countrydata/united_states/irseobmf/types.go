package irseobmf

const (
	SourceSlug = "united_states_irs_eo_bmf"
	SourceName = "IRS Exempt Organizations Business Master File (EO BMF)"
)

// DefaultDownloadURLs are the four regional EO BMF extracts. Concatenated they
// give national coverage of tax-exempt organizations.
var DefaultDownloadURLs = []string{
	"https://www.irs.gov/pub/irs-soi/eo1.csv",
	"https://www.irs.gov/pub/irs-soi/eo2.csv",
	"https://www.irs.gov/pub/irs-soi/eo3.csv",
	"https://www.irs.gov/pub/irs-soi/eo4.csv",
}

// csvColumns is the canonical EO BMF header (IRS Publication 5926). Download maps
// rows by column name, so a reordered header still parses; the names double as
// the JSON keys in the NDJSON snapshot.
var csvColumns = []string{
	"EIN", "NAME", "ICO", "STREET", "CITY", "STATE", "ZIP", "GROUP",
	"SUBSECTION", "AFFILIATION", "CLASSIFICATION", "RULING", "DEDUCTIBILITY",
	"FOUNDATION", "ACTIVITY", "ORGANIZATION", "STATUS", "TAX_PERIOD",
	"ASSET_CD", "INCOME_CD", "FILING_REQ_CD", "PF_FILING_REQ_CD", "ACCT_PD",
	"ASSET_AMT", "INCOME_AMT", "REVENUE_AMT", "NTEE_CD", "SORT_NAME",
}

// IrsEoBmfRecord is one EO BMF row. All 28 columns are kept as strings: EIN and
// ZIP carry significant leading zeros, RULING/TAX_PERIOD are YYYYMM, and the
// amount columns may be blank — none should be coerced to numbers at parse time.
type IrsEoBmfRecord struct {
	EIN            string `json:"EIN"`
	Name           string `json:"NAME"`
	ICO            string `json:"ICO"`
	Street         string `json:"STREET"`
	City           string `json:"CITY"`
	State          string `json:"STATE"`
	ZIP            string `json:"ZIP"`
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
	AssetCd        string `json:"ASSET_CD"`
	IncomeCd       string `json:"INCOME_CD"`
	FilingReqCd    string `json:"FILING_REQ_CD"`
	PfFilingReqCd  string `json:"PF_FILING_REQ_CD"`
	AcctPd         string `json:"ACCT_PD"`
	AssetAmt       string `json:"ASSET_AMT"`
	IncomeAmt      string `json:"INCOME_AMT"`
	RevenueAmt     string `json:"REVENUE_AMT"`
	NteeCd         string `json:"NTEE_CD"`
	SortName       string `json:"SORT_NAME"`
}

// recordFromRow builds a record from a CSV row using a column-name index so the
// mapping is resilient to column reordering and missing trailing columns.
func recordFromRow(row []string, index map[string]int) IrsEoBmfRecord {
	get := func(name string) string {
		if i, ok := index[name]; ok && i < len(row) {
			return row[i]
		}
		return ""
	}
	return IrsEoBmfRecord{
		EIN:            get("EIN"),
		Name:           get("NAME"),
		ICO:            get("ICO"),
		Street:         get("STREET"),
		City:           get("CITY"),
		State:          get("STATE"),
		ZIP:            get("ZIP"),
		Group:          get("GROUP"),
		Subsection:     get("SUBSECTION"),
		Affiliation:    get("AFFILIATION"),
		Classification: get("CLASSIFICATION"),
		Ruling:         get("RULING"),
		Deductibility:  get("DEDUCTIBILITY"),
		Foundation:     get("FOUNDATION"),
		Activity:       get("ACTIVITY"),
		Organization:   get("ORGANIZATION"),
		Status:         get("STATUS"),
		TaxPeriod:      get("TAX_PERIOD"),
		AssetCd:        get("ASSET_CD"),
		IncomeCd:       get("INCOME_CD"),
		FilingReqCd:    get("FILING_REQ_CD"),
		PfFilingReqCd:  get("PF_FILING_REQ_CD"),
		AcctPd:         get("ACCT_PD"),
		AssetAmt:       get("ASSET_AMT"),
		IncomeAmt:      get("INCOME_AMT"),
		RevenueAmt:     get("REVENUE_AMT"),
		NteeCd:         get("NTEE_CD"),
		SortName:       get("SORT_NAME"),
	}
}

func buildColumnIndex(header []string) map[string]int {
	index := make(map[string]int, len(header))
	for i, name := range header {
		index[name] = i
	}
	return index
}
