package rawstore

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestDownloadFileValidatesAndCommitsObject(t *testing.T) {
	body := []byte("complete pack bytes")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/crawls/path/records.pack" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Length", "19")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(body)
	}))
	defer server.Close()
	store, err := NewStore(context.Background(), StoreConfig{
		Endpoint: server.URL, Region: "us-east-1", Bucket: "crawls",
		AccessKey: "access", SecretKey: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "records.pack")
	descriptor := ObjectDescriptor{Key: "path/records.pack", SizeBytes: int64(len(body)), SHA256: ChecksumBytes(body)}
	if err := store.DownloadFile(context.Background(), descriptor, destination); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Fatalf("downloaded body=%q, want %q", got, body)
	}

	descriptor.SHA256 = ChecksumBytes([]byte("different"))
	invalidDestination := filepath.Join(t.TempDir(), "invalid.pack")
	if err := store.DownloadFile(context.Background(), descriptor, invalidDestination); err == nil {
		t.Fatal("checksum mismatch unexpectedly succeeded")
	}
	if _, err := os.Stat(invalidDestination); !os.IsNotExist(err) {
		t.Fatalf("invalid object was committed: %v", err)
	}
}
