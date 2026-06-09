package prhytj

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"

	"github.com/cockroachdb/errors"
)

type ParsedRecord struct {
	LineNumber  int64
	PayloadHash string
	Record      CompanyRecord
}

func ParseSnapshot(ctx context.Context, path string, handle func(ParsedRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "open PRH YTJ snapshot")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var lineNumber int64
	for scanner.Scan() {
		lineNumber++
		if err := ctx.Err(); err != nil {
			return err
		}
		rawLine := append([]byte(nil), scanner.Bytes()...)
		trimmed := bytes.TrimSpace(rawLine)
		if len(trimmed) == 0 {
			continue
		}
		var record CompanyRecord
		if err := json.Unmarshal(trimmed, &record); err != nil {
			return errors.Wrap(err, "decode PRH YTJ record")
		}
		sum := sha256.Sum256(rawLine)
		record.RawPayload = trimmed
		record.PayloadHash = hex.EncodeToString(sum[:])
		if err := handle(ParsedRecord{
			LineNumber:  lineNumber,
			PayloadHash: record.PayloadHash,
			Record:      record,
		}); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "scan PRH YTJ snapshot")
	}
	return nil
}
