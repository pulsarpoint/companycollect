package s3client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListBucketsReturnsVisibleBuckets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/", r.URL.Path)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner><ID>test-owner</ID><DisplayName>test-owner</DisplayName></Owner>
  <Buckets>
    <Bucket><Name>crawls</Name><CreationDate>2026-06-11T05:00:00.000Z</CreationDate></Bucket>
    <Bucket><Name>source-runs</Name><CreationDate>2026-06-11T06:00:00.000Z</CreationDate></Bucket>
  </Buckets>
</ListAllMyBucketsResult>`))
	}))
	t.Cleanup(server.Close)

	client, err := New(server.URL, "access", "secret", "crawls")
	require.NoError(t, err)

	buckets, err := client.ListBuckets(context.Background())
	require.NoError(t, err)
	require.Len(t, buckets, 2)
	require.Equal(t, "crawls", buckets[0].Name)
	require.Equal(t, "2026-06-11T05:00:00Z", buckets[0].CreationDate.Format("2006-01-02T15:04:05Z"))
	require.Equal(t, "source-runs", buckets[1].Name)
}

func TestListObjectsReturnsFoldersObjectsAndCursor(t *testing.T) {
	var seen url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		// AWS SDK v2 emits path-style bucket root requests without a trailing slash.
		require.Equal(t, "/crawls", r.URL.Path)
		seen = r.URL.Query()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>crawls</Name>
  <Prefix>brreg/</Prefix>
  <KeyCount>3</KeyCount>
  <MaxKeys>2</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>next-page</NextContinuationToken>
  <CommonPrefixes><Prefix>brreg/search/</Prefix></CommonPrefixes>
  <Contents>
    <Key>brreg/index.ndjson</Key>
    <LastModified>2026-06-11T05:10:00.000Z</LastModified>
    <ETag>&quot;etag-1&quot;</ETag>
    <Size>42</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>`))
	}))
	t.Cleanup(server.Close)

	client, err := New(server.URL, "access", "secret", "crawls")
	require.NoError(t, err)

	result, err := client.ListObjects(context.Background(), ListObjectsInput{
		Bucket:    "crawls",
		Prefix:    "brreg/",
		Delimiter: "/",
		Cursor:    "cursor-1",
		Limit:     2,
	})
	require.NoError(t, err)

	require.Equal(t, "2", seen.Get("max-keys"))
	require.Equal(t, "brreg/", seen.Get("prefix"))
	require.Equal(t, "/", seen.Get("delimiter"))
	require.Equal(t, "cursor-1", seen.Get("continuation-token"))
	require.Equal(t, "list-type=2", "list-type="+seen.Get("list-type"))

	require.Equal(t, "crawls", result.Bucket)
	require.Equal(t, "brreg/", result.Prefix)
	require.Equal(t, "/", result.Delimiter)
	require.Equal(t, "next-page", result.NextCursor)
	require.Equal(t, []Folder{{Prefix: "brreg/search/", Name: "search"}}, result.Folders)
	require.Len(t, result.Objects, 1)
	require.Equal(t, "brreg/index.ndjson", result.Objects[0].Key)
	require.Equal(t, "index.ndjson", result.Objects[0].Name)
	require.Equal(t, int64(42), result.Objects[0].SizeBytes)
	require.Equal(t, `"etag-1"`, result.Objects[0].ETag)
	require.Equal(t, "STANDARD", result.Objects[0].StorageClass)

	limit, err := strconv.Atoi(seen.Get("max-keys"))
	require.NoError(t, err)
	require.Equal(t, 2, limit)
}
