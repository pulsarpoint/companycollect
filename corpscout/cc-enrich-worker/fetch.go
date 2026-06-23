package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// rangeGetter fetches a byte range of an object. S3 in prod, a fake in tests.
type rangeGetter interface {
	GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error)
}

// FetchRecord fetches one WARC record (a single gzip member) by byte range and
// parses the embedded HTTP response into (headers, body). The record layout is:
// WARC headers \r\n\r\n  HTTP status+headers \r\n\r\n  HTTP body.
func FetchRecord(ctx context.Context, g rangeGetter, bucket, key string, offset, length int64) (http.Header, []byte, error) {
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

// s3Getter is the production rangeGetter backed by aws-sdk-go-v2.
type s3Getter struct{ client *s3.Client }

func NewS3Getter(ctx context.Context, region string) (s3Getter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return s3Getter{}, err
	}
	return s3Getter{client: s3.NewFromConfig(cfg)}, nil
}

func (s s3Getter) GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
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
