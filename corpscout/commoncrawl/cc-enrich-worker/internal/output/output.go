package output

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
)

// The row structs carry BOTH a `parquet` tag (for the file written by the run) and a `ch` tag
// (for the native-protocol INSERT done by the `load` command / internal/load). The two must name
// the same column; a test pins ch==parquet so they can't drift.

// DomainRow mirrors corpscout.commoncrawl_domains (migration 000046 + 000066 slim): the domain
// master/identity, one row per domain, written by EVERY pass. Classification moved to IndustryRow,
// page/decision signals to PageSignalRow, structured entities to JSONLDRow, contacts to ContactRow.
type DomainRow struct {
	CrawlID     string    `parquet:"crawl_id" ch:"crawl_id"`
	URL         string    `parquet:"url" ch:"url"`
	RootDomain  string    `parquet:"root_domain" ch:"root_domain"`
	Subdomain   string    `parquet:"subdomain" ch:"subdomain"`
	SourceURL   string    `parquet:"source_url" ch:"source_url"`
	SourceRunID string    `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt  time.Time `parquet:"resolved_at,timestamp" ch:"resolved_at"`
}

// IndustryRow mirrors corpscout.commoncrawl_industries (migration 000063): one row per
// (domain, nace_code) — a domain can have multiple industries. rank orders them, is_primary marks
// the headline. Written by the industry pass.
type IndustryRow struct {
	CrawlID      string    `parquet:"crawl_id" ch:"crawl_id"`
	RootDomain   string    `parquet:"root_domain" ch:"root_domain"`
	NaceCode     string    `parquet:"nace_code" ch:"nace_code"`
	NaceLabel    string    `parquet:"nace_label" ch:"nace_label"`
	NaceDivision string    `parquet:"nace_division" ch:"nace_division"`
	Rank         uint8     `parquet:"rank" ch:"rank"`
	IsPrimary    uint8     `parquet:"is_primary" ch:"is_primary"`
	Score        float32   `parquet:"score" ch:"score"`
	NaceMethod   string    `parquet:"nace_method" ch:"nace_method"`
	SourceURL    string    `parquet:"source_url" ch:"source_url"`
	SourceRunID  string    `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt   time.Time `parquet:"resolved_at,timestamp" ch:"resolved_at"`
}

// PageSignalRow mirrors corpscout.commoncrawl_page_signals (migration 000064): per-domain page
// classification + NACE ranking quality. Written by the industry pass.
type PageSignalRow struct {
	CrawlID       string    `parquet:"crawl_id" ch:"crawl_id"`
	RootDomain    string    `parquet:"root_domain" ch:"root_domain"`
	Subdomain     string    `parquet:"subdomain" ch:"subdomain"`
	SourceURL     string    `parquet:"source_url" ch:"source_url"`
	PageType      string    `parquet:"page_type" ch:"page_type"`
	PageTypeScore float32   `parquet:"page_type_score" ch:"page_type_score"`
	NaceConfident uint8     `parquet:"nace_confident" ch:"nace_confident"`
	NaceMargin    float32   `parquet:"nace_margin" ch:"nace_margin"`
	SourceRunID   string    `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt    time.Time `parquet:"resolved_at,timestamp" ch:"resolved_at"`
}

// TechRow mirrors corpscout.commoncrawl_page_technologies (migration 000125) column order.
type TechRow struct {
	CrawlID          string    `parquet:"crawl_id" ch:"crawl_id"`
	RootDomain       string    `parquet:"root_domain" ch:"root_domain"`
	PageURL          string    `parquet:"page_url" ch:"page_url"`
	Subdomain        string    `parquet:"subdomain" ch:"subdomain"`
	WarcIndex        uint32    `parquet:"warc_index" ch:"warc_index"`
	WarcFilename     string    `parquet:"warc_filename" ch:"warc_filename"`
	WarcRecordOffset uint64    `parquet:"warc_record_offset" ch:"warc_record_offset"`
	WarcRecordLength uint64    `parquet:"warc_record_length" ch:"warc_record_length"`
	Technology       string    `parquet:"technology" ch:"technology"`
	Category         string    `parquet:"category" ch:"category"`
	Version          string    `parquet:"version" ch:"version"`
	Confidence       uint8     `parquet:"confidence" ch:"confidence"`
	SourceRunID      string    `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt       time.Time `parquet:"resolved_at,timestamp" ch:"resolved_at"`
}

// IdentifierRow mirrors corpscout.commoncrawl_domain_identifiers (migration 000051):
// one row per (domain, identifier) scraped from a page (e.g. an LEI → GLEIF).
type IdentifierRow struct {
	CrawlID     string    `parquet:"crawl_id" ch:"crawl_id"`
	RootDomain  string    `parquet:"root_domain" ch:"root_domain"`
	URL         string    `parquet:"url" ch:"url"`
	Subdomain   string    `parquet:"subdomain" ch:"subdomain"`
	IDType      string    `parquet:"id_type" ch:"id_type"`
	IDValue     string    `parquet:"id_value" ch:"id_value"`
	Valid       uint8     `parquet:"valid" ch:"valid"`
	Source      string    `parquet:"source" ch:"source"`
	SourceURL   string    `parquet:"source_url" ch:"source_url"`
	SourceRunID string    `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt  time.Time `parquet:"resolved_at,timestamp" ch:"resolved_at"`
}

// JSONLDRow mirrors corpscout.commoncrawl_page_jsonld (migration 000127): one independently
// addressable JSON-LD entity with the exact page and WARC record that supplied it.
type JSONLDRow struct {
	CrawlID          string    `parquet:"crawl_id" ch:"crawl_id"`
	RootDomain       string    `parquet:"root_domain" ch:"root_domain"`
	PageURL          string    `parquet:"page_url" ch:"page_url"`
	Subdomain        string    `parquet:"subdomain" ch:"subdomain"`
	WarcIndex        uint32    `parquet:"warc_index" ch:"warc_index"`
	WarcFilename     string    `parquet:"warc_filename" ch:"warc_filename"`
	WarcRecordOffset uint64    `parquet:"warc_record_offset" ch:"warc_record_offset"`
	WarcRecordLength uint64    `parquet:"warc_record_length" ch:"warc_record_length"`
	ScriptIndex      uint32    `parquet:"script_index" ch:"script_index"`
	EntityPath       string    `parquet:"entity_path" ch:"entity_path"`
	EntityID         string    `parquet:"entity_id" ch:"entity_id"`
	EntityTypes      []string  `parquet:"entity_types" ch:"entity_types"`
	IsOrganization   uint8     `parquet:"is_organization" ch:"is_organization"`
	Name             string    `parquet:"name" ch:"name"`
	LegalName        string    `parquet:"legal_name" ch:"legal_name"`
	Description      string    `parquet:"description" ch:"description"`
	EntityURL        string    `parquet:"entity_url" ch:"entity_url"`
	Logo             string    `parquet:"logo" ch:"logo"`
	Email            string    `parquet:"email" ch:"email"`
	Telephone        string    `parquet:"telephone" ch:"telephone"`
	SameAs           []string  `parquet:"same_as" ch:"same_as"`
	Country          string    `parquet:"country" ch:"country"`
	FoundingYear     uint16    `parquet:"founding_year" ch:"founding_year"`
	EmployeeCount    uint32    `parquet:"employee_count" ch:"employee_count"`
	EntityJSON       string    `parquet:"entity_json" ch:"entity_json"`
	SourceRunID      string    `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt       time.Time `parquet:"resolved_at,timestamp" ch:"resolved_at"`
}

// ContactRow mirrors corpscout.commoncrawl_domain_contact_info (migration 000068): one row per
// (domain, contact_type, value) — many emails/phones/socials per domain. Written by the TECH pass.
type ContactRow struct {
	CrawlID     string    `parquet:"crawl_id" ch:"crawl_id"`
	RootDomain  string    `parquet:"root_domain" ch:"root_domain"`
	ContactType string    `parquet:"contact_type" ch:"contact_type"`
	Value       string    `parquet:"value" ch:"value"`
	Source      string    `parquet:"source" ch:"source"`
	SourceURL   string    `parquet:"source_url" ch:"source_url"`
	SourceRunID string    `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt  time.Time `parquet:"resolved_at,timestamp" ch:"resolved_at"`
}

// EmbeddingRow is the raw page embedding from the industry pass — the expensive GPU artifact, kept so
// we never have to recompute it (re-classification, similarity, Qdrant all derive from it on CPU).
// Stored as Parquet only (NOT ClickHouse), in a separate data/embedding/ tree. fp32, one row/domain.
// Self-describing: the (warc_filename, warc_offset, warc_length) triple re-fetches the EXACT archived
// page that produced this vector, without re-resolving the CommonCrawl index.
type EmbeddingRow struct {
	CrawlID      string    `parquet:"crawl_id"`
	RootDomain   string    `parquet:"root_domain"`
	Subdomain    string    `parquet:"subdomain"` // page host (apex/www -> "")
	Embedding    []float32 `parquet:"embedding"`
	EmbedDim     uint16    `parquet:"embed_dim"`
	TextLen      uint32    `parquet:"text_len"`      // chars embedded (capped at MAX_CHARS)
	SourceURL    string    `parquet:"source_url"`    // the page URL
	WarcFilename string    `parquet:"warc_filename"` // archived page location — re-fetch coords
	WarcOffset   int64     `parquet:"warc_offset"`
	WarcLength   int64     `parquet:"warc_length"`
	SourceRunID  string    `parquet:"source_run_id"`
	ResolvedAt   time.Time `parquet:"resolved_at,timestamp"`
}

// SecurityRow mirrors corpscout.commoncrawl_domain_security (migration 078): the full response-header
// map of a domain's primary page, captured verbatim. One row / domain (tech pass).
type SecurityRow struct {
	CrawlID     string            `parquet:"crawl_id" ch:"crawl_id"`
	RootDomain  string            `parquet:"root_domain" ch:"root_domain"`
	SourceURL   string            `parquet:"source_url" ch:"source_url"`
	Headers     map[string]string `parquet:"headers" ch:"headers"`
	SourceRunID string            `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt  time.Time         `parquet:"resolved_at,timestamp" ch:"resolved_at"`
}

// PageMetaRow mirrors corpscout.commoncrawl_domain_page_meta (migration 079): head-level content signals
// of a domain's primary page. One row / domain (tech pass).
type PageMetaRow struct {
	CrawlID     string            `parquet:"crawl_id" ch:"crawl_id"`
	RootDomain  string            `parquet:"root_domain" ch:"root_domain"`
	SourceURL   string            `parquet:"source_url" ch:"source_url"`
	Title       string            `parquet:"title" ch:"title"`
	Meta        map[string]string `parquet:"meta" ch:"meta"`
	Canonical   string            `parquet:"canonical" ch:"canonical"`
	Hreflang    []string          `parquet:"hreflang" ch:"hreflang"`
	JSONLDTypes []string          `parquet:"jsonld_types" ch:"jsonld_types"`
	Charset     string            `parquet:"charset" ch:"charset"`
	SourceRunID string            `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt  time.Time         `parquet:"resolved_at,timestamp" ch:"resolved_at"`
}

// Zstd is the compression codec for every parquet output — the text-heavy kinds compress 3-5x,
// and every reader in the chain (loader, DuckDB, pyarrow) decodes it transparently.
var Zstd = parquet.Compression(&parquet.Zstd)

func WriteDomains(path string, rows []DomainRow) error { return parquet.WriteFile(path, rows, Zstd) }
func WriteIndustries(path string, rows []IndustryRow) error {
	return parquet.WriteFile(path, rows, Zstd)
}
func WritePageSignals(path string, rows []PageSignalRow) error {
	return parquet.WriteFile(path, rows, Zstd)
}
func WriteContacts(path string, rows []ContactRow) error { return parquet.WriteFile(path, rows, Zstd) }
func WriteJSONLD(path string, rows []JSONLDRow) error    { return parquet.WriteFile(path, rows, Zstd) }
func WriteTech(path string, rows []TechRow) error        { return parquet.WriteFile(path, rows, Zstd) }
func WriteIdentifiers(path string, rows []IdentifierRow) error {
	return parquet.WriteFile(path, rows, Zstd)
}
func WriteEmbeddings(path string, rows []EmbeddingRow) error {
	return parquet.WriteFile(path, rows, Zstd)
}
func WriteSecurity(path string, rows []SecurityRow) error { return parquet.WriteFile(path, rows, Zstd) }
func WritePageMeta(path string, rows []PageMetaRow) error { return parquet.WriteFile(path, rows, Zstd) }

// UploadToS3 puts a local file at the given bucket/key.
func UploadToS3(ctx context.Context, client *s3.Client, bucket, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: f,
	})
	return err
}
