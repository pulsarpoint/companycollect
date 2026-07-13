package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"

	"cc-enrich-worker/internal/model"
)

const (
	readySchemaVersion    = 1
	maxReadyBytes         = 1 << 20
	catalogDownloadTries  = 3
	catalogCopyBufferSize = 1 << 20
	maxSidecarBytes       = 128
)

// S3Config contains the RustFS connection settings used to read a published catalog.
// BaseURI names the bucket and catalog prefix, for example s3://crawls/commoncrawl/catalogs.
type S3Config struct {
	BaseURI   string
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
}

type catalogLocation struct {
	bucket     string
	directory  string
	readyKey   string
	catalogKey string
}

type readyManifest struct {
	SchemaVersion int         `json:"schema_version"`
	CrawlID       string      `json:"crawl_id"`
	Selection     string      `json:"selection"`
	Catalog       readyObject `json:"catalog"`
}

type readyObject struct {
	Key       string `json:"key"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

// LoadS3WARC synchronizes the immutable RustFS catalog into the local cache and queries one WARC.
func LoadS3WARC(
	ctx context.Context,
	config S3Config,
	cacheBase, crawlID, selection string,
	warcIndex uint32,
) (Warc, []model.WorklistItem, error) {
	location, err := deriveCatalogLocation(config.BaseURI, crawlID, selection)
	if err != nil {
		return Warc{}, nil, err
	}
	client, err := newS3Client(ctx, config)
	if err != nil {
		return Warc{}, nil, err
	}
	path, err := ensureLocalCatalog(ctx, client, location, cacheBase, crawlID, selection)
	if err != nil {
		return Warc{}, nil, err
	}
	return LoadWARC(ctx, path, warcIndex)
}

// SyncLocal synchronizes the immutable RustFS catalog into the local cache and returns the local
// catalog path. It composes the same steps LoadS3WARC uses to prime the cache, but queries no WARC —
// operators call it to pre-warm the catalog on a freshly provisioned host before the first range run.
// It is idempotent: ensureLocalCatalog validates the cached SHA and skips the download whenever the
// local copy already matches the committed catalog.
func SyncLocal(
	ctx context.Context,
	config S3Config,
	cacheBase, crawlID, selection string,
) (string, error) {
	location, err := deriveCatalogLocation(config.BaseURI, crawlID, selection)
	if err != nil {
		return "", err
	}
	client, err := newS3Client(ctx, config)
	if err != nil {
		return "", err
	}
	return ensureLocalCatalog(ctx, client, location, cacheBase, crawlID, selection)
}

func ensureLocalCatalog(
	ctx context.Context,
	client *s3.Client,
	location catalogLocation,
	cacheBase,
	crawlID,
	selection string,
) (string, error) {
	if strings.TrimSpace(cacheBase) == "" {
		return "", errors.New("catalog cache base is required")
	}
	cacheDirectory := filepath.Join(cacheBase, crawlID, "warc-index", selection)
	if err := os.MkdirAll(cacheDirectory, 0o755); err != nil {
		return "", errors.Wrap(err, "create catalog cache directory")
	}

	lock, err := os.OpenFile(filepath.Join(cacheDirectory, ".catalog.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return "", errors.Wrap(err, "open catalog cache lock")
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		return "", errors.Wrap(err, "lock catalog cache")
	}

	manifest, err := readReadyManifest(ctx, client, location, crawlID, selection)
	if err != nil {
		return "", err
	}
	if err := validateCatalogObject(ctx, client, location.bucket, manifest.Catalog); err != nil {
		return "", err
	}

	catalogPath := filepath.Join(cacheDirectory, "catalog.duckdb")
	sidecarPath := catalogPath + ".sha256"
	partialPath := catalogPath + ".partial"
	partialSidecarPath := sidecarPath + ".partial"
	removePartials(partialPath, partialSidecarPath)
	if cachedCatalogMatches(catalogPath, sidecarPath, manifest.Catalog) {
		return catalogPath, nil
	}

	var downloadErr error
	for attempt := 1; attempt <= catalogDownloadTries; attempt++ {
		removePartials(partialPath, partialSidecarPath)
		downloadErr = downloadCatalog(ctx, client, location.bucket, manifest.Catalog, partialPath)
		if downloadErr == nil {
			break
		}
		if ctx.Err() != nil {
			removePartials(partialPath, partialSidecarPath)
			return "", errors.Wrap(ctx.Err(), "download catalog")
		}
	}
	if downloadErr != nil {
		removePartials(partialPath, partialSidecarPath)
		return "", errors.Wrapf(downloadErr, "download catalog after %d attempts", catalogDownloadTries)
	}
	if err := writeSidecar(partialSidecarPath, manifest.Catalog.SHA256); err != nil {
		removePartials(partialPath, partialSidecarPath)
		return "", err
	}
	if err := os.Rename(partialPath, catalogPath); err != nil {
		removePartials(partialPath, partialSidecarPath)
		return "", errors.Wrap(err, "publish cached catalog")
	}
	// Persist the catalog rename before publishing its checksum marker. After any crash, a matching
	// sidecar therefore cannot refer to the older catalog that occupied the same path.
	if err := syncDirectory(cacheDirectory); err != nil {
		removePartials(partialPath, partialSidecarPath)
		return "", err
	}
	if err := os.Rename(partialSidecarPath, sidecarPath); err != nil {
		removePartials(partialPath, partialSidecarPath)
		return "", errors.Wrap(err, "publish cached catalog checksum")
	}
	if err := syncDirectory(cacheDirectory); err != nil {
		return "", err
	}
	return catalogPath, nil
}

func cachedCatalogMatches(catalogPath, sidecarPath string, object readyObject) bool {
	catalogInfo, err := os.Lstat(catalogPath)
	if err != nil || !catalogInfo.Mode().IsRegular() || catalogInfo.Size() != object.SizeBytes {
		return false
	}
	sidecarInfo, err := os.Lstat(sidecarPath)
	if err != nil || !sidecarInfo.Mode().IsRegular() || sidecarInfo.Size() > maxSidecarBytes {
		return false
	}
	checksum, err := os.ReadFile(sidecarPath)
	return err == nil && strings.TrimSpace(string(checksum)) == object.SHA256
}

func downloadCatalog(
	ctx context.Context,
	client *s3.Client,
	bucket string,
	object readyObject,
	partialPath string,
) error {
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object.Key),
	})
	if err != nil {
		return errors.Wrapf(err, "get RustFS object %s", object.Key)
	}
	if output.ContentLength == nil || *output.ContentLength != object.SizeBytes {
		actualSize := int64(-1)
		if output.ContentLength != nil {
			actualSize = *output.ContentLength
		}
		_ = output.Body.Close()
		return errors.Newf(
			"RustFS object %s GET size is %d, catalog commit records %d",
			object.Key,
			actualSize,
			object.SizeBytes,
		)
	}

	partial, err := os.OpenFile(partialPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		_ = output.Body.Close()
		return errors.Wrap(err, "create partial catalog")
	}
	hash := sha256.New()
	written, copyErr := io.CopyBuffer(io.MultiWriter(partial, hash), output.Body, make([]byte, catalogCopyBufferSize))
	bodyCloseErr := output.Body.Close()
	if copyErr != nil {
		_ = partial.Close()
		return errors.Wrap(copyErr, "read catalog object")
	}
	if bodyCloseErr != nil {
		_ = partial.Close()
		return errors.Wrap(bodyCloseErr, "close catalog object")
	}
	if written != object.SizeBytes {
		_ = partial.Close()
		return errors.Newf("downloaded catalog size is %d, want %d", written, object.SizeBytes)
	}
	actualSHA256 := hex.EncodeToString(hash.Sum(nil))
	if actualSHA256 != object.SHA256 {
		_ = partial.Close()
		return errors.Newf("downloaded catalog sha256 is %s, want %s", actualSHA256, object.SHA256)
	}
	if err := partial.Sync(); err != nil {
		_ = partial.Close()
		return errors.Wrap(err, "fsync partial catalog")
	}
	if err := partial.Close(); err != nil {
		return errors.Wrap(err, "close partial catalog")
	}
	return nil
}

func writeSidecar(path, checksum string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return errors.Wrap(err, "create partial catalog checksum")
	}
	if _, err := io.WriteString(file, checksum+"\n"); err != nil {
		_ = file.Close()
		return errors.Wrap(err, "write partial catalog checksum")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errors.Wrap(err, "fsync partial catalog checksum")
	}
	if err := file.Close(); err != nil {
		return errors.Wrap(err, "close partial catalog checksum")
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "open catalog cache directory for fsync")
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return errors.Wrap(err, "fsync catalog cache directory")
	}
	if err := directory.Close(); err != nil {
		return errors.Wrap(err, "close catalog cache directory")
	}
	return nil
}

func removePartials(paths ...string) {
	for _, path := range paths {
		_ = os.Remove(path)
	}
}

func deriveCatalogLocation(baseURI, crawlID, selection string) (catalogLocation, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURI))
	if err != nil {
		return catalogLocation{}, errors.Wrap(err, "parse catalog S3 base")
	}
	if parsed.Scheme != "s3" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return catalogLocation{}, errors.Newf("catalog S3 base must be s3://bucket/prefix, got %q", baseURI)
	}
	if err := validateKeyComponent("crawl ID", crawlID); err != nil {
		return catalogLocation{}, err
	}
	if err := validateKeyComponent("selection", selection); err != nil {
		return catalogLocation{}, err
	}
	prefix := strings.Trim(parsed.Path, "/")
	if prefix != "" {
		for _, component := range strings.Split(prefix, "/") {
			if err := validateKeyComponent("catalog prefix", component); err != nil {
				return catalogLocation{}, err
			}
		}
	}
	components := []string{crawlID, selection}
	if prefix != "" {
		components = append([]string{prefix}, components...)
	}
	directory := strings.Join(components, "/")
	return catalogLocation{
		bucket:     parsed.Host,
		directory:  directory,
		readyKey:   directory + "/ready.json",
		catalogKey: directory + "/catalog.duckdb",
	}, nil
}

func validateKeyComponent(name, value string) error {
	if value == "" || value == "." || value == ".." || strings.ContainsAny(value, "/\\") {
		return errors.Newf("%s must be one non-empty S3 key component, got %q", name, value)
	}
	return nil
}

func newS3Client(ctx context.Context, config S3Config) (*s3.Client, error) {
	if strings.TrimSpace(config.Endpoint) == "" || strings.TrimSpace(config.Region) == "" {
		return nil, errors.New("CORPSCOUT_S3_ENDPOINT and CORPSCOUT_S3_REGION are required")
	}
	if strings.TrimSpace(config.AccessKey) == "" || strings.TrimSpace(config.SecretKey) == "" {
		return nil, errors.New("CORPSCOUT_S3_ACCESS_KEY and CORPSCOUT_S3_SECRET_KEY are required")
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(config.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, "")),
	)
	if err != nil {
		return nil, errors.Wrap(err, "load catalog RustFS configuration")
	}
	return s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(config.Endpoint)
		options.UsePathStyle = true
		options.DisableLogOutputChecksumValidationSkipped = true
	}), nil
}

func readReadyManifest(
	ctx context.Context,
	client *s3.Client,
	location catalogLocation,
	crawlID,
	selection string,
) (readyManifest, error) {
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(location.bucket),
		Key:    aws.String(location.readyKey),
	})
	if err != nil {
		return readyManifest{}, errors.Wrapf(err, "read catalog commit %s", location.readyKey)
	}
	body, readErr := io.ReadAll(io.LimitReader(output.Body, maxReadyBytes+1))
	closeErr := output.Body.Close()
	if readErr != nil {
		return readyManifest{}, errors.Wrapf(readErr, "read catalog commit body %s", location.readyKey)
	}
	if closeErr != nil {
		return readyManifest{}, errors.Wrapf(closeErr, "close catalog commit body %s", location.readyKey)
	}
	if len(body) > maxReadyBytes {
		return readyManifest{}, errors.Newf("catalog commit %s exceeds %d bytes", location.readyKey, maxReadyBytes)
	}
	return decodeReadyManifest(body, location, crawlID, selection)
}

func decodeReadyManifest(
	body []byte,
	location catalogLocation,
	crawlID,
	selection string,
) (readyManifest, error) {
	var manifest readyManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return readyManifest{}, errors.Wrap(err, "decode catalog commit")
	}
	if manifest.SchemaVersion != readySchemaVersion {
		return readyManifest{}, errors.Newf(
			"catalog commit schema_version=%d, want %d",
			manifest.SchemaVersion,
			readySchemaVersion,
		)
	}
	if manifest.CrawlID != crawlID || manifest.Selection != selection {
		return readyManifest{}, errors.Newf(
			"catalog commit identity is crawl=%q selection=%q, want crawl=%q selection=%q",
			manifest.CrawlID,
			manifest.Selection,
			crawlID,
			selection,
		)
	}
	if err := validateReadyObject("catalog", manifest.Catalog, location.catalogKey); err != nil {
		return readyManifest{}, err
	}
	return manifest, nil
}

func validateReadyObject(name string, object readyObject, expectedKey string) error {
	if object.Key != expectedKey {
		return errors.Newf("catalog commit %s key is %q, want %q", name, object.Key, expectedKey)
	}
	if object.SizeBytes <= 0 {
		return errors.Newf("catalog commit %s size_bytes must be positive, got %d", name, object.SizeBytes)
	}
	if !isLowerSHA256(object.SHA256) {
		return errors.Newf("catalog commit %s sha256 must be 64 lowercase hexadecimal characters", name)
	}
	return nil
}

func isLowerSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func validateCatalogObject(
	ctx context.Context,
	client *s3.Client,
	bucket string,
	object readyObject,
) error {
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object.Key),
	})
	if err != nil {
		return errors.Wrapf(err, "head RustFS object %s", object.Key)
	}
	actualSize := int64(-1)
	if head.ContentLength != nil {
		actualSize = *head.ContentLength
	}
	if actualSize != object.SizeBytes {
		return errors.Newf(
			"RustFS object %s size is %d, catalog commit records %d",
			object.Key,
			actualSize,
			object.SizeBytes,
		)
	}
	if !strings.EqualFold(head.Metadata["sha256"], object.SHA256) {
		return errors.Newf(
			"RustFS object %s sha256 metadata is %q, catalog commit records %q",
			object.Key,
			head.Metadata["sha256"],
			object.SHA256,
		)
	}
	return nil
}
