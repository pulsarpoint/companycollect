package rawstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"

	"github.com/cockroachdb/errors"
)

func ChecksumBytes(body []byte) SHA256 {
	sum := sha256.Sum256(body)
	return SHA256(hex.EncodeToString(sum[:]))
}

func ChecksumFile(path string) (SHA256, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, errors.Wrapf(err, "open checksum file %s", path)
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return "", 0, errors.Wrapf(err, "checksum file %s", path)
	}
	return SHA256(hex.EncodeToString(hash.Sum(nil))), size, nil
}

func EncodeChunkManifest(manifest ChunkManifest) ([]byte, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return encodeJSON(manifest)
}

func DecodeChunkManifest(body []byte) (ChunkManifest, error) {
	var manifest ChunkManifest
	if err := decodeJSON(body, &manifest); err != nil {
		return ChunkManifest{}, err
	}
	if err := manifest.Validate(); err != nil {
		return ChunkManifest{}, err
	}
	return manifest, nil
}

func EncodeReadyManifest(ready ReadyManifest) ([]byte, error) {
	if err := ready.Validate(); err != nil {
		return nil, err
	}
	return encodeJSON(ready)
}

func DecodeReadyManifest(body []byte) (ReadyManifest, error) {
	var ready ReadyManifest
	if err := decodeJSON(body, &ready); err != nil {
		return ReadyManifest{}, err
	}
	if err := ready.Validate(); err != nil {
		return ReadyManifest{}, err
	}
	return ready, nil
}

func encodeJSON(value any) ([]byte, error) {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, errors.Wrap(err, "encode JSON document")
	}
	return append(body, '\n'), nil
}

func decodeJSON(body []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errors.Wrap(err, "decode JSON document")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("JSON document contains trailing data")
	}
	return nil
}
