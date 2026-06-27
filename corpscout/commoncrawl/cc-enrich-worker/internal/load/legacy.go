package load

import (
	"context"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/output"
)

// LegacyDomainRow is the OLD fat commoncrawl_domains shape (pre-split, before migrations 063–066):
// classification + contacts + top-3 arrays all on one row. Old shard output
// (out_industry_*/domains.parquet) is in this shape. The loader detects it and fans it into the
// current split tables so old output loads into the new schema with no re-crawl — identical to what
// the current worker would have written.
type LegacyDomainRow struct {
	CrawlID        string    `parquet:"crawl_id"`
	URL            string    `parquet:"url"`
	RootDomain     string    `parquet:"root_domain"`
	Subdomain      string    `parquet:"subdomain"`
	Emails         []string  `parquet:"emails"`
	EmailCount     uint32    `parquet:"email_count"`
	PageType       string    `parquet:"page_type"`
	PageTypeScore  float32   `parquet:"page_type_score"`
	NaceCode       string    `parquet:"nace_code"`
	NaceLabel      string    `parquet:"nace_label"`
	NaceDivision   string    `parquet:"nace_division"`
	NaceConfident  uint8     `parquet:"nace_confident"`
	NaceConfidence float32   `parquet:"nace_confidence"`
	NaceMargin     float32   `parquet:"nace_margin"`
	NaceScore      float32   `parquet:"nace_score"`
	NaceMethod     string    `parquet:"nace_method"`
	Top3Codes      []string  `parquet:"nace_top3_codes"`
	Top3Labels     []string  `parquet:"nace_top3_labels"`
	Top3Scores     []float32 `parquet:"nace_top3_scores"`
	SourceURL      string    `parquet:"source_url"`
	SourceRunID    string    `parquet:"source_run_id"`
	ResolvedAt     time.Time `parquet:"resolved_at,timestamp"`
}

func b2u8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

// divisionOf is the NACE division: the part of a code before the first '.' ("29.31" -> "29").
func divisionOf(code string) string {
	for i := 0; i < len(code); i++ {
		if code[i] == '.' {
			return code[:i]
		}
	}
	return code
}

// domainsParquetIsLegacy reports whether a domains.parquet is the OLD fat shape, detected by the
// presence of the nace_top3_codes column (only the pre-split DomainRow carried it).
func domainsParquetIsLegacy(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return false, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return false, err
	}
	for _, col := range pf.Schema().Columns() {
		if len(col) > 0 && col[0] == "nace_top3_codes" {
			return true, nil
		}
	}
	return false, nil
}

// loadLegacyDomains reads an OLD fat domains.parquet and fans it into the split tables
// (commoncrawl_domains thin + commoncrawl_industries + commoncrawl_page_signals) — the same mapping
// the current worker applies, so a `load --dir` over old shard output produces identical rows.
func loadLegacyDomains(ctx context.Context, conn driver.Conn, path string) ([]Result, error) {
	rows, err := parquet.ReadFile[LegacyDomainRow](path)
	if err != nil {
		return nil, err
	}
	doms := make([]output.DomainRow, 0, len(rows))
	sigs := make([]output.PageSignalRow, 0, len(rows))
	var inds []output.IndustryRow
	var cons []output.ContactRow
	for i := range rows {
		r := &rows[i]
		doms = append(doms, output.DomainRow{
			CrawlID: r.CrawlID, URL: r.URL, RootDomain: r.RootDomain, Subdomain: r.Subdomain,
			SourceURL: r.SourceURL, SourceRunID: r.SourceRunID, ResolvedAt: r.ResolvedAt,
		})
		sigs = append(sigs, output.PageSignalRow{
			CrawlID: r.CrawlID, RootDomain: r.RootDomain, Subdomain: r.Subdomain, SourceURL: r.SourceURL,
			PageType: r.PageType, PageTypeScore: r.PageTypeScore, NaceConfident: r.NaceConfident, NaceMargin: r.NaceMargin,
			SourceRunID: r.SourceRunID, ResolvedAt: r.ResolvedAt,
		})
		n := len(r.Top3Codes)
		if len(r.Top3Labels) < n {
			n = len(r.Top3Labels)
		}
		if len(r.Top3Scores) < n {
			n = len(r.Top3Scores)
		}
		for j := 0; j < n; j++ {
			inds = append(inds, output.IndustryRow{
				CrawlID: r.CrawlID, RootDomain: r.RootDomain,
				NaceCode: r.Top3Codes[j], NaceLabel: r.Top3Labels[j], NaceDivision: divisionOf(r.Top3Codes[j]),
				Rank: uint8(j + 1), IsPrimary: b2u8(j == 0), Score: r.Top3Scores[j], NaceMethod: r.NaceMethod,
				SourceURL: r.SourceURL, SourceRunID: r.SourceRunID, ResolvedAt: r.ResolvedAt,
			})
		}
		for _, e := range r.Emails { // recover the industry-pass regex emails -> contacts
			cons = append(cons, output.ContactRow{
				CrawlID: r.CrawlID, RootDomain: r.RootDomain, ContactType: "email", Value: e, Source: "regex",
				SourceURL: r.SourceURL, SourceRunID: r.SourceRunID, ResolvedAt: r.ResolvedAt,
			})
		}
	}
	var out []Result
	nd, err := Insert(ctx, conn, Tables["domains"], doms)
	out = append(out, Result{Path: path, Table: Tables["domains"], Rows: nd})
	if err != nil {
		return out, err
	}
	ns, err := Insert(ctx, conn, Tables["page_signals"], sigs)
	out = append(out, Result{Path: path, Table: Tables["page_signals"], Rows: ns})
	if err != nil {
		return out, err
	}
	ni, err := Insert(ctx, conn, Tables["industries"], inds)
	out = append(out, Result{Path: path, Table: Tables["industries"], Rows: ni})
	if err != nil {
		return out, err
	}
	nc, err := Insert(ctx, conn, Tables["contacts"], cons)
	out = append(out, Result{Path: path, Table: Tables["contacts"], Rows: nc})
	return out, err
}
