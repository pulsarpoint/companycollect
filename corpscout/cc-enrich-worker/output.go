package main

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
)

// DomainRow mirrors corpscout.commoncrawl_domains (migration 000046) column order.
type DomainRow struct {
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

// TechRow mirrors corpscout.commoncrawl_technologies (migration 000047) column order.
type TechRow struct {
	CrawlID     string    `parquet:"crawl_id"`
	URL         string    `parquet:"url"`
	RootDomain  string    `parquet:"root_domain"`
	Subdomain   string    `parquet:"subdomain"`
	Technology  string    `parquet:"technology"`
	Category    string    `parquet:"category"`
	Version     string    `parquet:"version"`
	Confidence  uint8     `parquet:"confidence"`
	SourceURL   string    `parquet:"source_url"`
	SourceRunID string    `parquet:"source_run_id"`
	ResolvedAt  time.Time `parquet:"resolved_at,timestamp"`
}

// IdentifierRow mirrors corpscout.commoncrawl_company_identifiers (migration 000051):
// one row per (domain, identifier) scraped from a page (e.g. an LEI → GLEIF).
type IdentifierRow struct {
	CrawlID     string    `parquet:"crawl_id"`
	RootDomain  string    `parquet:"root_domain"`
	URL         string    `parquet:"url"`
	Subdomain   string    `parquet:"subdomain"`
	IDType      string    `parquet:"id_type"`
	IDValue     string    `parquet:"id_value"`
	Valid       uint8     `parquet:"valid"`
	Source      string    `parquet:"source"`
	SourceURL   string    `parquet:"source_url"`
	SourceRunID string    `parquet:"source_run_id"`
	ResolvedAt  time.Time `parquet:"resolved_at,timestamp"`
}

func WriteDomains(path string, rows []DomainRow) error         { return parquet.WriteFile(path, rows) }
func WriteTech(path string, rows []TechRow) error              { return parquet.WriteFile(path, rows) }
func WriteIdentifiers(path string, rows []IdentifierRow) error { return parquet.WriteFile(path, rows) }

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
