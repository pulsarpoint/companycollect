package load

import (
	"strings"
	"testing"

	"cc-dns-worker/internal/model"
)

func TestColumnLists(t *testing.T) {
	rc := chColumns[model.RecordRow]()
	if !strings.Contains(strings.Join(rc, ","), "record_type") || len(rc) < 11 {
		t.Errorf("RecordRow columns wrong: %v", rc)
	}
	sc := chColumns[model.ScanRow]()
	if !strings.Contains(strings.Join(sc, ","), "nameservers") || len(sc) < 13 {
		t.Errorf("ScanRow columns wrong: %v", sc)
	}
}
