package rawstore

import (
	"fmt"
	"regexp"

	"github.com/cockroachdb/errors"
)

const rawPrefix = "commoncrawl/raw"

var (
	crawlIDPattern   = regexp.MustCompile(`^CC-MAIN-[0-9]{4}-[0-9]{2}$`)
	selectionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
)

type ChunkObjectKeys struct {
	Pack     string
	Index    string
	Manifest string
}

func ValidatePartIdentity(crawlID, selection string, part int) error {
	if !crawlIDPattern.MatchString(crawlID) {
		return errors.Newf("invalid crawl ID %q", crawlID)
	}
	if !selectionPattern.MatchString(selection) || selection == "." || selection == ".." {
		return errors.Newf("invalid selection %q", selection)
	}
	if part < 0 {
		return errors.Newf("part must be non-negative, got %d", part)
	}
	return nil
}

func RawPartPrefix(crawlID, selection string, part int) (string, error) {
	if err := ValidatePartIdentity(crawlID, selection, part); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/crawl=%s/selection=%s/part=%03d", rawPrefix, crawlID, selection, part), nil
}

func KeysForChunk(crawlID, selection string, part, chunk int) (ChunkObjectKeys, error) {
	prefix, err := RawPartPrefix(crawlID, selection, part)
	if err != nil {
		return ChunkObjectKeys{}, err
	}
	if chunk < 0 {
		return ChunkObjectKeys{}, errors.Newf("chunk must be non-negative, got %d", chunk)
	}
	prefix = fmt.Sprintf("%s/chunk=%06d", prefix, chunk)
	return ChunkObjectKeys{
		Pack:     prefix + "/records.pack",
		Index:    prefix + "/index.parquet",
		Manifest: prefix + "/manifest.json",
	}, nil
}
