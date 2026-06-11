package s3client

import (
	"bytes"
	"context"
	"io"
	"path"
	"strings"
	"time"

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

type Bucket struct {
	Name         string    `json:"name"`
	CreationDate time.Time `json:"creation_date"`
}

type Folder struct {
	Prefix string `json:"prefix"`
	Name   string `json:"name"`
}

type Object struct {
	Key          string    `json:"key"`
	Name         string    `json:"name"`
	SizeBytes    int64     `json:"size_bytes"`
	LastModified time.Time `json:"last_modified"`
	ETag         string    `json:"etag"`
	StorageClass string    `json:"storage_class"`
}

type ListObjectsInput struct {
	Bucket    string
	Prefix    string
	Delimiter string
	Cursor    string
	Limit     int32
}

type ListObjectsResult struct {
	Bucket     string   `json:"bucket"`
	Prefix     string   `json:"prefix"`
	Delimiter  string   `json:"delimiter"`
	NextCursor string   `json:"next_cursor"`
	Folders    []Folder `json:"folders"`
	Objects    []Object `json:"objects"`
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

func (c *Client) ListBuckets(ctx context.Context) ([]Bucket, error) {
	out, err := c.s3.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, errors.Wrap(err, "s3 list buckets")
	}
	buckets := make([]Bucket, 0, len(out.Buckets))
	for _, bucket := range out.Buckets {
		buckets = append(buckets, Bucket{
			Name:         aws.ToString(bucket.Name),
			CreationDate: aws.ToTime(bucket.CreationDate),
		})
	}
	return buckets, nil
}

func (c *Client) ListObjects(ctx context.Context, input ListObjectsInput) (ListObjectsResult, error) {
	bucket := strings.TrimSpace(input.Bucket)
	if bucket == "" {
		return ListObjectsResult{}, errors.New("s3 list objects bucket is required")
	}
	delimiter := input.Delimiter
	if delimiter == "" {
		delimiter = "/"
	}
	limit := input.Limit
	if limit <= 0 {
		limit = 100
	}
	out, err := c.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		Prefix:            aws.String(input.Prefix),
		Delimiter:         aws.String(delimiter),
		ContinuationToken: stringPointer(input.Cursor),
		MaxKeys:           aws.Int32(limit),
	})
	if err != nil {
		return ListObjectsResult{}, errors.Wrap(err, "s3 list objects "+bucket)
	}

	result := ListObjectsResult{
		Bucket:     bucket,
		Prefix:     input.Prefix,
		Delimiter:  delimiter,
		NextCursor: aws.ToString(out.NextContinuationToken),
		Folders:    make([]Folder, 0, len(out.CommonPrefixes)),
		Objects:    make([]Object, 0, len(out.Contents)),
	}
	for _, commonPrefix := range out.CommonPrefixes {
		prefix := aws.ToString(commonPrefix.Prefix)
		result.Folders = append(result.Folders, Folder{
			Prefix: prefix,
			Name:   prefixDisplayName(input.Prefix, prefix),
		})
	}
	for _, object := range out.Contents {
		key := aws.ToString(object.Key)
		if key == input.Prefix {
			continue
		}
		result.Objects = append(result.Objects, Object{
			Key:          key,
			Name:         objectDisplayName(input.Prefix, key),
			SizeBytes:    aws.ToInt64(object.Size),
			LastModified: aws.ToTime(object.LastModified),
			ETag:         aws.ToString(object.ETag),
			StorageClass: string(object.StorageClass),
		})
	}
	return result, nil
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

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func prefixDisplayName(parentPrefix, prefix string) string {
	trimmed := strings.TrimSuffix(strings.TrimPrefix(prefix, parentPrefix), "/")
	if trimmed == "" {
		return prefix
	}
	return path.Base(trimmed)
}

func objectDisplayName(parentPrefix, key string) string {
	trimmed := strings.TrimPrefix(key, parentPrefix)
	if trimmed == "" {
		return key
	}
	return path.Base(trimmed)
}
