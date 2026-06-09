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

func ParseSnapshot(ctx context.Context, path string, handle func(CompanyRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "open PRH YTJ snapshot")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		raw = append([]byte(nil), raw...)
		var record CompanyRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return errors.Wrap(err, "decode PRH YTJ record")
		}
		sum := sha256.Sum256(raw)
		record.RawPayload = raw
		record.PayloadHash = hex.EncodeToString(sum[:])
		if err := handle(record); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "scan PRH YTJ snapshot")
	}
	return nil
}
