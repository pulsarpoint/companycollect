package coloradoentities

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

func ParseSnapshot(ctx context.Context, path string, handle func(ColoradoEntityRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "open Colorado entities snapshot")
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
		var record ColoradoEntityRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return errors.Wrap(err, "decode Colorado entity record")
		}
		sum := sha256.Sum256(raw)
		record.RawPayload = raw
		record.PayloadHash = hex.EncodeToString(sum[:])
		if err := handle(record); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "scan Colorado entities snapshot")
	}
	return nil
}
