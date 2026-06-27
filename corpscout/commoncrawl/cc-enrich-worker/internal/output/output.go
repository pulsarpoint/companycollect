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
// page/decision signals to PageSignalRow, self-reported "about" to MetadataRow, contacts to ContactRow.
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

// TechRow mirrors corpscout.commoncrawl_technologies (migration 000047) column order.
type TechRow struct {
	CrawlID     string    `parquet:"crawl_id" ch:"crawl_id"`
	URL         string    `parquet:"url" ch:"url"`
	RootDomain  string    `parquet:"root_domain" ch:"root_domain"`
	Subdomain   string    `parquet:"subdomain" ch:"subdomain"`
	Technology  string    `parquet:"technology" ch:"technology"`
	Category    string    `parquet:"category" ch:"category"`
	Version     string    `parquet:"version" ch:"version"`
	Confidence  uint8     `parquet:"confidence" ch:"confidence"`
	SourceURL   string    `parquet:"source_url" ch:"source_url"`
	SourceRunID string    `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt  time.Time `parquet:"resolved_at,timestamp" ch:"resolved_at"`
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

// MetadataRow mirrors corpscout.commoncrawl_domain_metadata (migration 000067): one row per domain,
// what a domain's pages say ABOUT THEMSELVES (schema.org Organization JSON-LD). Self-reported, not
// verified. Contacts moved to ContactRow; authoritative company facts are the external company master.
type MetadataRow struct {
	CrawlID       string    `parquet:"crawl_id" ch:"crawl_id"`
	RootDomain    string    `parquet:"root_domain" ch:"root_domain"`
	Subdomain     string    `parquet:"subdomain" ch:"subdomain"`
	Name          string    `parquet:"name" ch:"name"`
	Description   string    `parquet:"description" ch:"description"`
	Logo          string    `parquet:"logo" ch:"logo"`
	Country       string    `parquet:"country" ch:"country"`
	FoundingYear  uint16    `parquet:"founding_year" ch:"founding_year"`
	EmployeeCount uint32    `parquet:"employee_count" ch:"employee_count"`
	Source        string    `parquet:"source" ch:"source"`
	SourceURL     string    `parquet:"source_url" ch:"source_url"`
	SourceRunID   string    `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt    time.Time `parquet:"resolved_at,timestamp" ch:"resolved_at"`
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
type EmbeddingRow struct {
	CrawlID     string    `parquet:"crawl_id"`
	RootDomain  string    `parquet:"root_domain"`
	Embedding   []float32 `parquet:"embedding"`
	EmbedDim    uint16    `parquet:"embed_dim"`
	SourceURL   string    `parquet:"source_url"`
	SourceRunID string    `parquet:"source_run_id"`
	ResolvedAt  time.Time `parquet:"resolved_at,timestamp"`
}

func WriteDomains(path string, rows []DomainRow) error         { return parquet.WriteFile(path, rows) }
func WriteIndustries(path string, rows []IndustryRow) error    { return parquet.WriteFile(path, rows) }
func WritePageSignals(path string, rows []PageSignalRow) error { return parquet.WriteFile(path, rows) }
func WriteContacts(path string, rows []ContactRow) error       { return parquet.WriteFile(path, rows) }
func WriteMetadata(path string, rows []MetadataRow) error      { return parquet.WriteFile(path, rows) }
func WriteTech(path string, rows []TechRow) error              { return parquet.WriteFile(path, rows) }
func WriteIdentifiers(path string, rows []IdentifierRow) error { return parquet.WriteFile(path, rows) }
func WriteEmbeddings(path string, rows []EmbeddingRow) error   { return parquet.WriteFile(path, rows) }

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
