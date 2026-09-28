package knowledge

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"provider_recon/internal/model"
	"provider_recon/internal/publish"
)

func putStore(t *testing.T, store publish.FSStore, providers []string, docs ...model.Document) {
	t.Helper()
	ctx := context.Background()
	idx, _ := json.Marshal(publish.RunIndex{Runs: []publish.RunSummary{}, Providers: providers})
	if err := store.Put(ctx, publish.IndexKey, idx, "application/json"); err != nil {
		t.Fatal(err)
	}
	for _, d := range docs {
		b, _ := json.Marshal(d)
		if err := store.Put(ctx, publish.LatestKey(d.Slug), b, "application/json"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLoadStoreCompilesTheListedProviders(t *testing.T) {
	store := publish.FSStore{Root: t.TempDir()}
	putStore(t, store, []string{"a", "b"},
		doc("a", []string{"a.com"}, svc("a.dns", []string{"dns"}, rule("NS", "target", "suffix", "ns.a.com", 0, 1))),
		doc("b", []string{"b.com"}, svc("b.dns", []string{"dns"})))
	idx, digest, err := LoadStore(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if m, ok := idx.Match(NSTarget, "x.ns.a.com"); !ok || m.ServiceKey != "a.dns" {
		t.Fatalf("match = %+v %v", m, ok)
	}
	if p, ok := idx.ProviderForKey("b.com"); !ok || p.Slug != "b" || idx.Documents() != 2 {
		t.Fatalf("provider = %+v %v, documents = %d", p, ok, idx.Documents())
	}
	if !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("digest = %q", digest)
	}
	_, again, err := LoadStore(context.Background(), store)
	if err != nil || again != digest {
		t.Fatalf("digest not stable: %v %q %q", err, again, digest)
	}
}

func TestLoadStoreFailsOnMissingIndexOrDocument(t *testing.T) {
	if _, _, err := LoadStore(context.Background(), publish.FSStore{Root: t.TempDir()}); err == nil {
		t.Error("missing index loaded")
	}
	store := publish.FSStore{Root: t.TempDir()}
	putStore(t, store, []string{"a", "gone"}, doc("a", nil, svc("a.dns", []string{"dns"})))
	if _, _, err := LoadStore(context.Background(), store); err == nil || !strings.Contains(err.Error(), "gone") {
		t.Errorf("missing document: err = %v", err)
	}
}
