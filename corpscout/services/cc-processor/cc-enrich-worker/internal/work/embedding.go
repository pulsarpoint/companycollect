package work

import (
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
)

// parquetRows returns a Parquet file's row count by reading only its footer. It errors if the file is
// missing or not a valid/complete Parquet (e.g. a write killed mid-flush) — used by embed verify-and-skip.
func parquetRows(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return 0, err
	}
	return pf.NumRows(), nil
}

// CompletedEmbedding reports whether dir already holds a complete vector file under EITHER name —
// embeddings.parquet (fp32) or embeddings_fp16.parquet (converted offline). A zero-row file counts
// only with its .empty completion marker; an fp16 file parquet-go cannot decode counts when
// non-empty (its conversion step verified it before the fp32 was pruned).
func CompletedEmbedding(dir string) (string, int64, bool) {
	for _, name := range []string{"embeddings.parquet", "embeddings_fp16.parquet"} {
		path := filepath.Join(dir, name)
		rows, err := parquetRows(path)
		if err == nil {
			if rows > 0 {
				return path, rows, true
			}
			if _, markerErr := os.Stat(path + ".empty"); markerErr == nil {
				return path, 0, true
			}
			continue
		}
		// Converted fp16 files from the older toolchain may have a logical type parquet-go cannot
		// decode. Their conversion step verified the file before removing fp32, so retain that fallback.
		if name == "embeddings_fp16.parquet" {
			info, statErr := os.Stat(path)
			if statErr == nil && info.Size() > 0 {
				return path, 0, true
			}
		}
	}
	return "", 0, false
}
