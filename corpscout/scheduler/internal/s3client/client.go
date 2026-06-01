package s3client

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cockroachdb/errors"
)

// Client wraps the AWS S3 client with bucket-scoped operations.
type Client struct {
	s3     *s3.Client
	bucket string
}

// New creates an S3-compatible client using static credentials and a custom endpoint.
// UsePathStyle is enabled, which is required for S3-compatible stores such as rustfs/minio.
func New(endpoint, accessKey, secretKey, bucket string) (*Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, errors.Wrap(err, "s3client: load config")
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
	return &Client{s3: client, bucket: bucket}, nil
}

// EnsureBucket creates the configured bucket if it does not already exist.
func (c *Client) EnsureBucket(ctx context.Context) error {
	_, err := c.s3.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err != nil {
		var ae interface{ ErrorCode() string }
		if errors.As(err, &ae) {
			code := ae.ErrorCode()
			if code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists" {
				return nil
			}
		}
		return errors.Wrap(err, "s3 create bucket")
	}
	return nil
}

// Upload stores body under key in the configured bucket with the given content type.
func (c *Client) Upload(ctx context.Context, key string, body []byte, contentType string) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	return errors.Wrap(err, "s3 put object "+key)
}

// Download retrieves the object at key and returns its bytes and content type.
func (c *Client) Download(ctx context.Context, key string) ([]byte, string, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", errors.Wrap(err, "s3 get object "+key)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", errors.Wrap(err, "s3 read body "+key)
	}
	ct := ""
	if out.ContentType != nil {
		ct = *out.ContentType
	}
	return data, ct, nil
}
