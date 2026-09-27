package publish

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestS3StoreRoundTrip runs against the real object store only when
// PROVIDER_RECON_S3_IT=1 and CORPSCOUT_S3_* are set. It uses bucket
// provider-recon-it and removes what it writes.
func TestS3StoreRoundTrip(t *testing.T) {
	if os.Getenv("PROVIDER_RECON_S3_IT") != "1" {
		t.Skip("set PROVIDER_RECON_S3_IT=1 to run against the object store")
	}
	ctx := context.Background()
	cfg, err := S3ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Bucket = "provider-recon-it"
	s, err := NewS3Store(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket not idempotent: %v", err)
	}
	key := fmt.Sprintf("it/%d/x.json", time.Now().UnixNano())
	defer s.Delete(ctx, key)

	if _, ok, err := s.Get(ctx, key); err != nil || ok {
		t.Fatalf("missing key: ok=%v err=%v", ok, err)
	}
	if err := s.Put(ctx, key, []byte(`{"a":1}`), "application/json"); err != nil {
		t.Fatal(err)
	}
	b, ok, err := s.Get(ctx, key)
	if err != nil || !ok || string(b) != `{"a":1}` {
		t.Fatalf("get: %q %v %v", b, ok, err)
	}
}
