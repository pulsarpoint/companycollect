package model

type Technology struct {
	Name, Category, Version string
}

// Reference: NACE matrix, rows co-ordered with Codes/Labels/Divisions, L2-normalized.
type Reference struct {
	Codes, Labels, Divisions []string
	M                        [][]float32 // len = N x dim
}

// Prototypes: page-type vectors, Labels[i] is the class of row i, L2-normalized.
type Prototypes struct {
	Labels []string
	P      [][]float32
}

type DomainResult struct {
	CrawlID, RootDomain, URL, Subdomain string
	Emails                              []string
	PageType                            string
	PageTypeScore                       float32
	NaceCode, NaceLabel, NaceDivision   string
	NaceConfident                       bool
	NaceConfidence                      float32 // softmax(top-K standardized scores) in [0,1]
	NaceMargin, NaceScore               float32
	NaceMethod                          string
	Top3Codes, Top3Labels               []string
	Top3Scores                          []float32
	Tech                                []Technology // unioned across the domain's K pages
}

// Identifier is a company identifier scraped from a page that links the domain to an
// authoritative registry (e.g. an LEI → GLEIF). Extensible to VAT / registration numbers.
type Identifier struct {
	Type   string // "lei"
	Value  string
	Valid  bool   // checksum/format validated
	Source string // "jsonld" | "text"
}

// CompanyProfile is the firmographic record distilled from a page's schema.org
// Organization / LocalBusiness JSON-LD (clean structured data, when present).
type CompanyProfile struct {
	Name          string
	Description   string
	Logo          string
	Country       string // ISO alpha-2 when given as a code, else the raw value
	Email         string
	Phone         string
	FoundingYear  uint16
	EmployeeCount uint32
	SameAs        []string // linkedin / wikidata / crunchbase / socials
}

func (p CompanyProfile) Empty() bool {
	return p.Name == "" && p.Description == "" && p.Logo == "" && p.Country == "" &&
		p.Email == "" && p.Phone == "" && p.FoundingYear == 0 && p.EmployeeCount == 0 && len(p.SameAs) == 0
}

// WorklistItem is one row of the index-driven worklist (top-K pages per domain).
type WorklistItem struct {
	RootDomain, URL, WarcFilename string
	Offset, Length                int64
	Primary                       bool // rn == 1 (the shallowest page; gets embedded)
}
