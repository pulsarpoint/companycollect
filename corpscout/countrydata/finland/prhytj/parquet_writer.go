package prhytj

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
)

func WriteParquetRows[T any](path string, rows []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Wrap(err, "create parquet directory")
	}
	tempPath := path + ".tmp"
	if err := parquet.WriteFile(tempPath, rows); err != nil {
		_ = os.Remove(tempPath)
		return errors.Wrap(err, "write parquet file")
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return errors.Wrap(err, "rename parquet file")
	}
	return nil
}
