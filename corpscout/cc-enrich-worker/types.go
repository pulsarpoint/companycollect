package main

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

type PageFetch struct {
	URL, RootDomain, Subdomain string
	Primary                    bool
	Text                       string
	Emails, Socials            []string
	Tech                       []Technology
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
