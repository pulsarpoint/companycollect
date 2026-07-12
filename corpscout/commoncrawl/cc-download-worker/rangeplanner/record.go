package rangeplanner

import (
	"math"
	"strings"

	"github.com/cockroachdb/errors"
)

type Record struct {
	ID       int64
	WARCFile string
	Offset   int64
	Length   int64
}

func validateRecord(record Record) error {
	if strings.TrimSpace(record.WARCFile) == "" || record.Offset < 0 || record.Length <= 0 {
		return errors.Newf("invalid record id=%d WARC=%q offset=%d length=%d", record.ID, record.WARCFile, record.Offset, record.Length)
	}
	if record.Length > math.MaxInt64-record.Offset {
		return errors.Newf("record range overflows id=%d offset=%d length=%d", record.ID, record.Offset, record.Length)
	}
	return nil
}
