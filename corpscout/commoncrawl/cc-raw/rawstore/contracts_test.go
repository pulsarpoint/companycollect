package rawstore

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestGoldenChunkManifest(t *testing.T) {
	var manifest ChunkManifest
	decodeFixture(t, "chunk_manifest.json", &manifest)
	if err := manifest.Validate(); err != nil {
		t.Fatalf("validate golden manifest: %v", err)
	}

	keys, err := KeysForChunk(manifest.CrawlID, manifest.Selection, manifest.Part, manifest.Chunk)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Pack.Key != keys.Pack || manifest.Index.Key != keys.Index {
		t.Fatalf("manifest keys do not match generated keys: %+v", keys)
	}
}

func TestGoldenReadyManifest(t *testing.T) {
	var ready ReadyManifest
	decodeFixture(t, "ready.json", &ready)
	if err := ready.Validate(); err != nil {
		t.Fatalf("validate golden ready manifest: %v", err)
	}
}

func TestManifestDocumentsRoundTripAndRejectUnknownFields(t *testing.T) {
	var manifest ChunkManifest
	decodeFixture(t, "chunk_manifest.json", &manifest)
	body, err := EncodeChunkManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeChunkManifest(body)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, manifest) {
		t.Fatalf("manifest round trip mismatch\ngot:  %#v\nwant: %#v", decoded, manifest)
	}

	unknownField := bytes.Replace(body, []byte(`"schema_version": 1,`), []byte(`"schema_version": 1, "unexpected": true,`), 1)
	if _, err := DecodeChunkManifest(unknownField); err == nil {
		t.Fatal("manifest with unknown field passed decoding")
	}
	if _, err := DecodeChunkManifest(append(body, []byte("{}")...)); err == nil {
		t.Fatal("manifest with trailing JSON passed decoding")
	}
}

func TestReadyManifestMatchesCommittedChunkManifests(t *testing.T) {
	var ready ReadyManifest
	decodeFixture(t, "ready.json", &ready)
	var manifest ChunkManifest
	decodeFixture(t, "chunk_manifest.json", &manifest)
	manifestInfo, err := os.Stat(filepath.Join("testdata", "chunk_manifest.json"))
	if err != nil {
		t.Fatal(err)
	}

	committed := []CommittedChunkManifest{{
		Manifest:          manifest,
		ManifestSHA256:    ready.Chunks[0].ManifestSHA256,
		ManifestSizeBytes: manifestInfo.Size(),
	}}
	if err := ready.ValidateCommittedChunks(committed); err != nil {
		t.Fatalf("validate ready against committed chunks: %v", err)
	}

	committed[0].Manifest.Results.DownloadedRecords--
	committed[0].Manifest.Results.FailedRecords++
	committed[0].Manifest.Results.Errors.Other++
	if err := ready.ValidateCommittedChunks(committed); err == nil {
		t.Fatal("ready totals differing from committed chunk manifests passed validation")
	}
}

func TestGoldenIndexRowsRoundTripParquet(t *testing.T) {
	var rows []IndexRow
	decodeFixture(t, "index_rows.json", &rows)
	for i, row := range rows {
		if err := row.Validate(); err != nil {
			t.Fatalf("validate index row %d: %v", i, err)
		}
	}

	path := filepath.Join(t.TempDir(), "index.parquet")
	if err := parquet.WriteFile(path, rows); err != nil {
		t.Fatalf("write index parquet: %v", err)
	}
	got, err := parquet.ReadFile[IndexRow](path)
	if err != nil {
		t.Fatalf("read index parquet: %v", err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("index parquet round trip mismatch\ngot:  %#v\nwant: %#v", got, rows)
	}

	var manifest ChunkManifest
	decodeFixture(t, "chunk_manifest.json", &manifest)
	if err := ValidateIndexRows(got, manifest); err != nil {
		t.Fatalf("validate index against chunk manifest: %v", err)
	}
}

func TestChunkManifestRejectsContractViolations(t *testing.T) {
	tests := map[string]func(*ChunkManifest){
		"unknown schema":     func(m *ChunkManifest) { m.SchemaVersion++ },
		"wrong pack key":     func(m *ChunkManifest) { m.Pack.Key += ".partial" },
		"uncovered failure":  func(m *ChunkManifest) { m.Results.Errors.Timeout = 0 },
		"recompressed bytes": func(m *ChunkManifest) { m.Results.SourceBytes++ },
		"time runs backward": func(m *ChunkManifest) { m.Download.CompletedAt = m.Download.StartedAt.Add(-1) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			var manifest ChunkManifest
			decodeFixture(t, "chunk_manifest.json", &manifest)
			mutate(&manifest)
			if err := manifest.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestReadyManifestRejectsCoverageGaps(t *testing.T) {
	var ready ReadyManifest
	decodeFixture(t, "ready.json", &ready)
	ready.Chunks[0].FirstOrdinal = 1
	if err := ready.Validate(); err == nil || !strings.Contains(err.Error(), "ordinal range") {
		t.Fatalf("expected ordinal range error, got %v", err)
	}
}

func TestIndexRowRejectsInconsistentDownloadState(t *testing.T) {
	var rows []IndexRow
	decodeFixture(t, "index_rows.json", &rows)

	errorCode := "timeout"
	rows[0].ErrorCode = &errorCode
	if err := rows[0].Validate(); err == nil {
		t.Fatal("downloaded row with an error code passed validation")
	}

	packOffset := int64(0)
	rows[1].PackOffset = &packOffset
	if err := rows[1].Validate(); err == nil {
		t.Fatal("failed row with pack coordinates passed validation")
	}
}

func TestIndexRowsRejectNonContiguousPack(t *testing.T) {
	var rows []IndexRow
	decodeFixture(t, "index_rows.json", &rows)
	var manifest ChunkManifest
	decodeFixture(t, "chunk_manifest.json", &manifest)

	*rows[2].PackOffset = 401
	if err := ValidateIndexRows(rows, manifest); err == nil {
		t.Fatal("non-contiguous pack offsets passed validation")
	}
}

func TestKeyConstructionRejectsPathComponents(t *testing.T) {
	if _, err := RawPartPrefix("CC-MAIN-2026-25", "../tech25", 0); err == nil {
		t.Fatal("path-like selection passed validation")
	}
	if _, err := KeysForChunk("CC-MAIN-2026-25", "tech25", 0, -1); err == nil {
		t.Fatal("negative chunk passed validation")
	}
}

func decodeFixture(t *testing.T, name string, destination any) {
	t.Helper()
	file, err := os.Open(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("%s contains trailing JSON", name)
	}
}
