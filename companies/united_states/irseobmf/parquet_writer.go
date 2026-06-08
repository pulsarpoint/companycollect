package irseobmf

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
)

// WriteParquetRows writes rows to a parquet file via a unique temp file that is
// closed before rename. The temp file is removed on any write/close failure.
func WriteParquetRows[T any](path string, rows []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Wrap(err, "create parquet directory")
	}
	tempFile, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return errors.Wrap(err, "create temporary parquet file")
	}
	tempPath := tempFile.Name()
	removeTemp := func() {
		_ = os.Remove(tempPath)
	}

	writeErr := parquet.Write(tempFile, rows)
	closeErr := tempFile.Close()
	if writeErr != nil {
		removeTemp()
		return errors.Wrap(errors.CombineErrors(writeErr, closeErr), "write parquet file")
	}
	if closeErr != nil {
		removeTemp()
		return errors.Wrap(closeErr, "close parquet file")
	}
	if err := os.Rename(tempPath, path); err != nil {
		removeTemp()
		return errors.Wrap(err, "rename parquet file")
	}
	return nil
}
