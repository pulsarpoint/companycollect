package control

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

// TestConcurrentWritersDoNotLock verifies two Stores on the same file can write
// concurrently without SQLITE_BUSY (WAL + busy_timeout).
func TestConcurrentWritersDoNotLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	ctx := context.Background()

	open := func() *Store {
		s, err := Open(ctx, path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	}
	a, b := open(), open()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	write := func(s *Store, id string) {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			w := model.WorkUnit{ID: id, LogName: id, StartIndex: 0, EndIndex: 1000}
			if _, _, err := s.EnsureWorkUnit(ctx, w); err != nil {
				errs <- err
				return
			}
			if err := s.SaveProgress(ctx, id, int64(i), 1, 1); err != nil {
				errs <- err
				return
			}
		}
	}
	wg.Add(2)
	go write(a, "log-a")
	go write(b, "log-b")
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent write failed: %v", err)
	}
}
