package worker

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/output"
)

// ShardStreamer writes each output kind incrementally — one parquet row group per chunk — so the chunked
// tech/both path never buffers a whole part's rows in RAM. Peak memory becomes O(chunk), not O(part):
// without this a 250k-domain (~2.75M-page) shard needs ~80GB+, which no box RAM covers.
//
// Each kind is written to "<kind>.parquet.partial" and atomically renamed on Commit; Abort discards the
// partials so a part that fails the write contract leaves nothing behind. The output is an ordinary
// multi-row-group parquet, byte-compatible with the old single-shot output.WriteX funcs, so the loader is
// unchanged.
type ShardStreamer struct {
	all []committer // every opened kind, in write order, for Commit/Abort

	domains     *kindStream[output.DomainRow]
	industries  *kindStream[output.IndustryRow]
	pageSignals *kindStream[output.PageSignalRow]
	tech        *kindStream[output.TechRow]
	idents      *kindStream[output.IdentifierRow]
	metadata    *kindStream[output.MetadataRow]
	contacts    *kindStream[output.ContactRow]
	security    *kindStream[output.SecurityRow]
	pageMeta    *kindStream[output.PageMetaRow]
	embeddings  *kindStream[output.EmbeddingRow]

	domainCount int
	embedCount  int
}

// NewShardStreamer opens writers for exactly the kinds the run produces — mirroring the old write block:
// domains always; industries/page_signals (+ embeddings, in embedDir) when runEmbed; the tech kinds when
// runTech. Pass embedDir="" to skip embeddings. On any open error every partial opened so far is removed.
func NewShardStreamer(dir, embedDir string, runEmbed, runTech bool) (s *ShardStreamer, err error) {
	s = &ShardStreamer{}
	defer func() {
		if err != nil {
			s.Abort()
			s = nil
		}
	}()
	if s.domains, err = newKindStream[output.DomainRow](dir, "domains.parquet"); err != nil {
		return
	}
	s.all = append(s.all, s.domains)
	if runEmbed {
		if s.industries, err = newKindStream[output.IndustryRow](dir, "industries.parquet"); err != nil {
			return
		}
		s.all = append(s.all, s.industries)
		if s.pageSignals, err = newKindStream[output.PageSignalRow](dir, "page_signals.parquet"); err != nil {
			return
		}
		s.all = append(s.all, s.pageSignals)
	}
	if runTech {
		if s.tech, err = newKindStream[output.TechRow](dir, "tech.parquet"); err != nil {
			return
		}
		s.all = append(s.all, s.tech)
		if s.idents, err = newKindStream[output.IdentifierRow](dir, "identifiers.parquet"); err != nil {
			return
		}
		s.all = append(s.all, s.idents)
		if s.metadata, err = newKindStream[output.MetadataRow](dir, "metadata.parquet"); err != nil {
			return
		}
		s.all = append(s.all, s.metadata)
		if s.contacts, err = newKindStream[output.ContactRow](dir, "contacts.parquet"); err != nil {
			return
		}
		s.all = append(s.all, s.contacts)
		if s.security, err = newKindStream[output.SecurityRow](dir, "security.parquet"); err != nil {
			return
		}
		s.all = append(s.all, s.security)
		if s.pageMeta, err = newKindStream[output.PageMetaRow](dir, "page_meta.parquet"); err != nil {
			return
		}
		s.all = append(s.all, s.pageMeta)
	}
	if runEmbed && embedDir != "" {
		if s.embeddings, err = newKindStream[output.EmbeddingRow](embedDir, "embeddings.parquet"); err != nil {
			return
		}
		s.all = append(s.all, s.embeddings)
	}
	return s, nil
}

// Write appends one chunk's rows to each open kind (one row group each). Empty kind-slices are no-ops.
func (s *ShardStreamer) Write(res ShardResult) error {
	n, err := s.domains.write(res.Domains)
	if err != nil {
		return err
	}
	s.domainCount += n
	if s.industries != nil {
		if _, err := s.industries.write(res.Industries); err != nil {
			return err
		}
	}
	if s.pageSignals != nil {
		if _, err := s.pageSignals.write(res.PageSignals); err != nil {
			return err
		}
	}
	if s.tech != nil {
		if _, err := s.tech.write(res.Tech); err != nil {
			return err
		}
	}
	if s.idents != nil {
		if _, err := s.idents.write(res.Identifiers); err != nil {
			return err
		}
	}
	if s.metadata != nil {
		if _, err := s.metadata.write(res.Metadata); err != nil {
			return err
		}
	}
	if s.contacts != nil {
		if _, err := s.contacts.write(res.Contacts); err != nil {
			return err
		}
	}
	if s.security != nil {
		if _, err := s.security.write(res.Security); err != nil {
			return err
		}
	}
	if s.pageMeta != nil {
		if _, err := s.pageMeta.write(res.PageMeta); err != nil {
			return err
		}
	}
	if s.embeddings != nil {
		n, err := s.embeddings.write(res.Embeddings)
		if err != nil {
			return err
		}
		s.embedCount += n
	}
	return nil
}

// DomainCount / EmbeddingCount are the running totals written so far (for the write contract + logging).
func (s *ShardStreamer) DomainCount() int    { return s.domainCount }
func (s *ShardStreamer) EmbeddingCount() int { return s.embedCount }

// Commit closes every writer and atomically renames each ".partial" to its final path; returns the paths.
func (s *ShardStreamer) Commit() ([]string, error) {
	var paths []string
	for _, c := range s.all {
		if err := c.commit(); err != nil {
			return paths, err
		}
		paths = append(paths, c.name())
	}
	return paths, nil
}

// Abort discards all partial files. Safe to call multiple times / after a partial Commit.
func (s *ShardStreamer) Abort() {
	for _, c := range s.all {
		c.abort()
	}
}

// committer lets ShardStreamer hold the heterogeneous kindStream[T]s in one slice.
type committer interface {
	commit() error
	abort()
	name() string
}

// kindStream streams one output kind to a parquet file via a GenericWriter, flushing a row group per chunk.
type kindStream[T any] struct {
	path string // final path (without the .partial suffix)
	f    *os.File
	w    *parquet.GenericWriter[T]
}

func newKindStream[T any](dir, file string) (*kindStream[T], error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, file)
	f, err := os.Create(p + ".partial")
	if err != nil {
		return nil, err
	}
	return &kindStream[T]{path: p, f: f, w: parquet.NewGenericWriter[T](f)}, nil
}

func (k *kindStream[T]) write(rows []T) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if _, err := k.w.Write(rows); err != nil {
		return 0, fmt.Errorf("write %s: %w", k.path, err)
	}
	// Flush ends the current row group and releases the writer's in-memory buffer -> peak stays O(chunk).
	if err := k.w.Flush(); err != nil {
		return 0, fmt.Errorf("flush %s: %w", k.path, err)
	}
	return len(rows), nil
}

func (k *kindStream[T]) commit() error {
	if err := k.w.Close(); err != nil {
		return fmt.Errorf("close writer %s: %w", k.path, err)
	}
	if err := k.f.Close(); err != nil {
		return fmt.Errorf("close file %s: %w", k.path, err)
	}
	return os.Rename(k.path+".partial", k.path)
}

func (k *kindStream[T]) abort() {
	_ = k.w.Close()
	_ = k.f.Close()
	_ = os.Remove(k.path + ".partial")
}

func (k *kindStream[T]) name() string { return k.path }
