package fetch

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// RangeGetter fetches a byte range of an object. S3 in prod, a fake in tests.
type RangeGetter interface {
	GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error)
}

// FetchRecord fetches one WARC record (a single gzip member) by byte range and
// parses the embedded HTTP response into (headers, body). The record layout is:
// WARC headers \r\n\r\n  HTTP status+headers \r\n\r\n  HTTP body.
func FetchRecord(ctx context.Context, g RangeGetter, bucket, key string, offset, length int64) (http.Header, []byte, error) {
	raw, err := g.GetRange(ctx, bucket, key, offset, offset+length-1)
	if err != nil {
		return nil, nil, err
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, nil, fmt.Errorf("gzip: %w", err)
	}
	rec, err := io.ReadAll(gz)
	if err != nil {
		return nil, nil, fmt.Errorf("read record: %w", err)
	}
	// Skip the WARC header block (up to its blank-line terminator).
	idx := bytes.Index(rec, []byte("\r\n\r\n"))
	if idx < 0 {
		return nil, nil, fmt.Errorf("no WARC header terminator")
	}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(rec[idx+4:])), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("http parse: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("read body: %w", err)
	}
	return resp.Header, body, nil
}

// S3Getter is the production RangeGetter backed by aws-sdk-go-v2.
type S3Getter struct{ Client *s3.Client }

// NewS3Getter signs requests (in-AWS instance role / configured creds). CommonCrawl's
// S3 API denies anonymous access — off-AWS use httpRangeGetter instead.
func NewS3Getter(ctx context.Context, region string) (RangeGetter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return &S3Getter{}, err
	}
	// Validate creds upfront so we fail fast with a clear message off-AWS instead of
	// hanging on the 169.254 IMDS lookup for every fetch.
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := cfg.Credentials.Retrieve(cctx); err != nil {
		return &S3Getter{}, fmt.Errorf("no usable AWS credentials — export AWS_ACCESS_KEY_ID + "+
			"AWS_SECRET_ACCESS_KEY (or run with --s3-anonymous): %w", err)
	}
	return &S3Getter{Client: s3.NewFromConfig(cfg)}, nil
}

func (s *S3Getter) GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// httpRangeGetter fetches byte ranges over CommonCrawl's anonymous HTTPS CDN
// (data.commoncrawl.org), the only anonymous path — the S3 API denies unsigned reads.
type httpRangeGetter struct {
	base   string
	client *http.Client
}

func NewHTTPGetter(base string, concurrency int) RangeGetter {
	if base == "" {
		base = "https://data.commoncrawl.org/"
	}
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	if concurrency <= 0 {
		concurrency = 16
	}
	return httpRangeGetter{base: base, client: &http.Client{Transport: &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency * 2,
		MaxConnsPerHost:     concurrency,
	}}}
}

func (g httpRangeGetter) GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	rng := fmt.Sprintf("bytes=%d-%d", start, end)
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 { // exponential backoff for transient throttling (429/5xx)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt*attempt) * 250 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.base+key, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Range", rng)
		resp, err := g.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
			b, rerr := io.ReadAll(resp.Body)
			resp.Body.Close()
			return b, rerr
		}
		status := resp.StatusCode
		resp.Body.Close()
		lastErr = fmt.Errorf("http %d fetching %s", status, key)
		if status != http.StatusTooManyRequests && status < 500 {
			return nil, lastErr // 403/404 etc. are permanent — don't retry
		}
	}
	return nil, lastErr
}
