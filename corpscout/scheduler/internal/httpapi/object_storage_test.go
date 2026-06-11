package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	"github.com/pulsarpoint/corpscout/scheduler/internal/s3client"
)

func TestListObjectStorageBucketsReturnsBuckets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/", r.URL.Path)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner><ID>test-owner</ID><DisplayName>test-owner</DisplayName></Owner>
  <Buckets>
    <Bucket><Name>crawls</Name><CreationDate>2026-06-11T05:00:00.000Z</CreationDate></Bucket>
  </Buckets>
</ListAllMyBucketsResult>`))
	}))
	t.Cleanup(server.Close)

	client, err := s3client.New(server.URL, "access", "secret", "crawls")
	require.NoError(t, err)

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, client, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/object-storage/buckets", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"items":[{"name":"crawls","creation_date":"2026-06-11T05:00:00Z"}]}`, w.Body.String())
}

func TestListObjectStorageObjectsParsesQueryAndCapsLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/crawls", r.URL.Path)
		require.Equal(t, "2", r.URL.Query().Get("list-type"))
		require.Equal(t, "brreg/", r.URL.Query().Get("prefix"))
		require.Equal(t, "/", r.URL.Query().Get("delimiter"))
		require.Equal(t, "1000", r.URL.Query().Get("max-keys"))
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>crawls</Name>
  <Prefix>brreg/</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
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

	client, err := s3client.New(server.URL, "access", "secret", "crawls")
	require.NoError(t, err)

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, client, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/object-storage/buckets/crawls/objects?prefix=brreg/&limit=5000", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{
		"bucket":"crawls",
		"prefix":"brreg/",
		"delimiter":"/",
		"next_cursor":"",
		"folders":[{"prefix":"brreg/search/","name":"search"}],
		"objects":[{
			"key":"brreg/index.ndjson",
			"name":"index.ndjson",
			"size_bytes":42,
			"last_modified":"2026-06-11T05:10:00Z",
			"etag":"\"etag-1\"",
			"storage_class":"STANDARD"
		}]
	}`, w.Body.String())
}

func TestObjectStorageEndpointsRequireS3Client(t *testing.T) {
	r := routerFor(newTestHandlers(&stubQuerier{}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/object-storage/buckets", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.JSONEq(t, `{"error":"object storage client not configured"}`, w.Body.String())
}
