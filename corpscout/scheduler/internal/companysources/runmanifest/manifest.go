package runmanifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
)

const FileName = "manifest.json"

type Manifest struct {
	Country      string    `json:"country"`
	Source       string    `json:"source"`
	RunID        string    `json:"run_id"`
	DownloadedAt time.Time `json:"downloaded_at"`
	Files        []File    `json:"files"`
}

type File struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Rows   int64  `json:"rows"`
	SHA256 string `json:"sha256"`
}

func Write(runDir string, manifest Manifest) error {
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return errors.Wrap(err, "create run directory")
	}
	body, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal manifest")
	}
	body = append(body, '\n')
	if err := os.WriteFile(filepath.Join(runDir, FileName), body, 0o644); err != nil {
		return errors.Wrap(err, "write manifest")
	}
	return nil
}

func Read(runDir string) (Manifest, error) {
	body, err := os.ReadFile(filepath.Join(runDir, FileName))
	if err != nil {
		return Manifest{}, errors.Wrap(err, "read manifest")
	}
	var manifest Manifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return Manifest{}, errors.Wrap(err, "decode manifest")
	}
	return manifest, nil
}

func Hash(runDir string) (string, error) {
	body, err := os.ReadFile(filepath.Join(runDir, FileName))
	if err != nil {
		return "", errors.Wrap(err, "read manifest for hash")
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}
