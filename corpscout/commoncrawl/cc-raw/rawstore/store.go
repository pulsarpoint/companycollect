package rawstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/transport/http"
	"github.com/cockroachdb/errors"
)

type StoreConfig struct {
	Endpoint  string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
}

// Store is the concrete S3-compatible boundary shared by the downloader,
// staged processors, and raw-state command.
type Store struct {
	client *s3.Client
	bucket string
}

func NewStore(ctx context.Context, config StoreConfig) (*Store, error) {
	if strings.TrimSpace(config.Endpoint) == "" || strings.TrimSpace(config.Region) == "" || strings.TrimSpace(config.Bucket) == "" {
		return nil, errors.New("RustFS endpoint, region, and bucket are required")
	}
	if strings.TrimSpace(config.AccessKey) == "" || strings.TrimSpace(config.SecretKey) == "" {
		return nil, errors.New("RustFS access and secret keys are required")
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(config.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, "")),
	)
	if err != nil {
		return nil, errors.Wrap(err, "load RustFS client configuration")
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(config.Endpoint)
		options.UsePathStyle = true
		options.DisableLogOutputChecksumValidationSkipped = true
	})
	return &Store{client: client, bucket: config.Bucket}, nil
}

func (store *Store) PutFile(ctx context.Context, key, path, contentType string, checksum SHA256) error {
	if err := checksum.Validate(); err != nil {
		return errors.Wrap(err, "object SHA-256")
	}
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrapf(err, "open upload file %s", path)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return errors.Wrapf(err, "stat upload file %s", path)
	}
	_, err = store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(store.bucket),
		Key:           aws.String(key),
		Body:          file,
		ContentLength: aws.Int64(info.Size()),
		ContentType:   aws.String(contentType),
		Metadata:      map[string]string{"sha256": string(checksum)},
	})
	if err != nil {
		return errors.Wrapf(err, "put RustFS object %s", key)
	}
	return nil
}

func (store *Store) PutBytes(ctx context.Context, key, contentType string, body []byte, checksum SHA256) error {
	if err := checksum.Validate(); err != nil {
		return errors.Wrap(err, "object SHA-256")
	}
	_, err := store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(store.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		ContentType:   aws.String(contentType),
		Metadata:      map[string]string{"sha256": string(checksum)},
	})
	if err != nil {
		return errors.Wrapf(err, "put RustFS object %s", key)
	}
	return nil
}

func (store *Store) ReadBytes(ctx context.Context, key string) ([]byte, error) {
	output, err := store.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(store.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "get RustFS object %s", key)
	}
	defer output.Body.Close()
	body, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "read RustFS object %s", key)
	}
	return body, nil
}

func (store *Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := store.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(store.bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, errors.Wrapf(err, "head RustFS object %s", key)
}

func (store *Store) ObjectMatches(ctx context.Context, object ObjectDescriptor) (bool, error) {
	if err := object.SHA256.Validate(); err != nil {
		return false, errors.Wrap(err, "expected object SHA-256")
	}
	output, err := store.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(store.bucket),
		Key:    aws.String(object.Key),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "head RustFS object %s", object.Key)
	}
	checksum := output.Metadata["sha256"]
	return output.ContentLength != nil && *output.ContentLength == object.SizeBytes && checksum == string(object.SHA256), nil
}

func (store *Store) Delete(ctx context.Context, key string) error {
	_, err := store.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(store.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return errors.Wrapf(err, "delete RustFS object %s", key)
	}
	return nil
}

func isNotFound(err error) bool {
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "404", "NoSuchKey", "NotFound":
			return true
		}
	}
	var responseError *http.ResponseError
	return errors.As(err, &responseError) && responseError.HTTPStatusCode() == 404
}
