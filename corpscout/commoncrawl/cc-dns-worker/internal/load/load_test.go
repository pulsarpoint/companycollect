package load

import (
	"strings"
	"testing"

	"cc-dns-worker/internal/model"
)

func TestColumnLists(t *testing.T) {
	rc := chColumns[model.RecordRow]()
	if !strings.Contains(strings.Join(rc, ","), "first_seen") || len(rc) != 13 {
		t.Errorf("RecordRow columns wrong: %v", rc)
	}
	sc := chColumns[model.ScanRow]()
	if !strings.Contains(strings.Join(sc, ","), "nameservers") || len(sc) != 15 {
		t.Errorf("ScanRow columns wrong: %v", sc)
	}
}
