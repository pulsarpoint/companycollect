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

// DomainRow mirrors corpscout.commoncrawl_domains (migration 000046) column order.
type DomainRow struct {
	CrawlID        string    `parquet:"crawl_id" ch:"crawl_id"`
	URL            string    `parquet:"url" ch:"url"`
	RootDomain     string    `parquet:"root_domain" ch:"root_domain"`
	Subdomain      string    `parquet:"subdomain" ch:"subdomain"`
	Emails         []string  `parquet:"emails" ch:"emails"`
	EmailCount     uint32    `parquet:"email_count" ch:"email_count"`
	PageType       string    `parquet:"page_type" ch:"page_type"`
	PageTypeScore  float32   `parquet:"page_type_score" ch:"page_type_score"`
	NaceCode       string    `parquet:"nace_code" ch:"nace_code"`
	NaceLabel      string    `parquet:"nace_label" ch:"nace_label"`
	NaceDivision   string    `parquet:"nace_division" ch:"nace_division"`
	NaceConfident  uint8     `parquet:"nace_confident" ch:"nace_confident"`
	NaceConfidence float32   `parquet:"nace_confidence" ch:"nace_confidence"`
	NaceMargin     float32   `parquet:"nace_margin" ch:"nace_margin"`
	NaceScore      float32   `parquet:"nace_score" ch:"nace_score"`
	NaceMethod     string    `parquet:"nace_method" ch:"nace_method"`
	Top3Codes      []string  `parquet:"nace_top3_codes" ch:"nace_top3_codes"`
	Top3Labels     []string  `parquet:"nace_top3_labels" ch:"nace_top3_labels"`
	Top3Scores     []float32 `parquet:"nace_top3_scores" ch:"nace_top3_scores"`
	SourceURL      string    `parquet:"source_url" ch:"source_url"`
	SourceRunID    string    `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt     time.Time `parquet:"resolved_at,timestamp" ch:"resolved_at"`
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

// IdentifierRow mirrors corpscout.commoncrawl_company_identifiers (migration 000051):
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

// ProfileRow mirrors corpscout.commoncrawl_company_profile (migration 000053): one row
// per domain, the firmographics distilled from schema.org Organization JSON-LD.
type ProfileRow struct {
	CrawlID       string    `parquet:"crawl_id" ch:"crawl_id"`
	RootDomain    string    `parquet:"root_domain" ch:"root_domain"`
	URL           string    `parquet:"url" ch:"url"`
	Subdomain     string    `parquet:"subdomain" ch:"subdomain"`
	Name          string    `parquet:"name" ch:"name"`
	Description   string    `parquet:"description" ch:"description"`
	Logo          string    `parquet:"logo" ch:"logo"`
	Country       string    `parquet:"country" ch:"country"`
	Email         string    `parquet:"email" ch:"email"`
	Phone         string    `parquet:"phone" ch:"phone"`
	FoundingYear  uint16    `parquet:"founding_year" ch:"founding_year"`
	EmployeeCount uint32    `parquet:"employee_count" ch:"employee_count"`
	SameAs        []string  `parquet:"same_as" ch:"same_as"`
	Source        string    `parquet:"source" ch:"source"`
	SourceURL     string    `parquet:"source_url" ch:"source_url"`
	SourceRunID   string    `parquet:"source_run_id" ch:"source_run_id"`
	ResolvedAt    time.Time `parquet:"resolved_at,timestamp" ch:"resolved_at"`
}

func WriteDomains(path string, rows []DomainRow) error         { return parquet.WriteFile(path, rows) }
func WriteTech(path string, rows []TechRow) error              { return parquet.WriteFile(path, rows) }
func WriteIdentifiers(path string, rows []IdentifierRow) error { return parquet.WriteFile(path, rows) }
func WriteProfiles(path string, rows []ProfileRow) error       { return parquet.WriteFile(path, rows) }

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
