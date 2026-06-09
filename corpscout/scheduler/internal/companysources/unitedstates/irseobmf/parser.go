package irseobmf

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
)

func ParseSnapshot(ctx context.Context, path string, handle func(IrsEoBmfRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "open IRS EO BMF snapshot")
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
		var record IrsEoBmfRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return errors.Wrap(err, "decode IRS EO BMF record")
		}
		record.EIN = normalizeEIN(record.EIN)
		if record.EIN == "" {
			return errors.New("decode IRS EO BMF record: missing EIN")
		}
		sum := sha256.Sum256(raw)
		record.RawPayload = raw
		record.PayloadHash = hex.EncodeToString(sum[:])
		if err := handle(record); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "scan IRS EO BMF snapshot")
	}
	return nil
}

func normalizeEIN(ein string) string {
	ein = strings.TrimSpace(ein)
	if ein == "" {
		return ""
	}
	if len(ein) < 9 && isAllDigits(ein) {
		return strings.Repeat("0", 9-len(ein)) + ein
	}
	return ein
}

func isAllDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
