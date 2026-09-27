package publish

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// DefaultBucket holds provider-recon output unless PROVIDER_RECON_BUCKET overrides it.
const DefaultBucket = "provider-recon"

// S3Config addresses the corpscout object store (RustFS, S3 API).
type S3Config struct {
	Endpoint, AccessKey, SecretKey, Bucket, Region string
}

// S3ConfigFromEnv reads the shared CORPSCOUT_S3_* variables.
func S3ConfigFromEnv() (S3Config, error) {
	c := S3Config{
		Endpoint:  os.Getenv("CORPSCOUT_S3_ENDPOINT"),
		AccessKey: os.Getenv("CORPSCOUT_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("CORPSCOUT_S3_SECRET_KEY"),
		Bucket:    os.Getenv("PROVIDER_RECON_BUCKET"),
		Region:    "us-east-1",
	}
	if c.Bucket == "" {
		c.Bucket = DefaultBucket
	}
	if c.Endpoint == "" || c.AccessKey == "" || c.SecretKey == "" {
		return c, errors.New("CORPSCOUT_S3_ENDPOINT, CORPSCOUT_S3_ACCESS_KEY and CORPSCOUT_S3_SECRET_KEY must be set")
	}
	return c, nil
}

// S3Store implements Store on one bucket.
type S3Store struct {
	client *s3.Client
	bucket string
}

// NewS3Store builds a path-style client for the S3-compatible endpoint.
func NewS3Store(ctx context.Context, c S3Config) (*S3Store, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(c.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")),
	)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(c.Endpoint)
		o.UsePathStyle = true
		// S3-compatible servers can reject the SDK's default trailing checksums.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &S3Store{client: client, bucket: c.Bucket}, nil
}

// EnsureBucket creates the bucket when missing.
func (s *S3Store) EnsureBucket(ctx context.Context) error {
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(s.bucket)})
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "BucketAlreadyOwnedByYou" || apiErr.ErrorCode() == "BucketAlreadyExists") {
		return nil
	}
	return fmt.Errorf("create bucket %s: %w", s.bucket, err)
}

// Get implements Store.
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, bool, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get s3://%s/%s: %w", s.bucket, key, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// Put implements Store.
func (s *S3Store) Put(ctx context.Context, key string, body []byte, contentType string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
		Body: bytes.NewReader(body), ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("put s3://%s/%s: %w", s.bucket, key, err)
	}
	return nil
}

// Delete removes one object (used by the integration test).
func (s *S3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	return err
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var re *awshttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == 404
}
