package rawstate

import (
	"fmt"
	"regexp"

	"cc-enrich-worker/internal/rawstore"
	"github.com/cockroachdb/errors"
)

const statePrefix = "commoncrawl/state"

var processorPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

type ProcessorObjectKeys struct {
	Processing string
	Processed  string
	Loaded     string
}

func StatePartPrefix(crawlID, selection string, part int) (string, error) {
	if err := rawstore.ValidatePartIdentity(crawlID, selection, part); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/crawl=%s/selection=%s/part=%03d", statePrefix, crawlID, selection, part), nil
}

func DownloadReadyKey(crawlID, selection string, part int) (string, error) {
	prefix, err := StatePartPrefix(crawlID, selection, part)
	if err != nil {
		return "", err
	}
	return prefix + "/download/ready.json", nil
}

func KeysForProcessor(crawlID, selection string, part int, processor string) (ProcessorObjectKeys, error) {
	prefix, err := StatePartPrefix(crawlID, selection, part)
	if err != nil {
		return ProcessorObjectKeys{}, err
	}
	if !processorPattern.MatchString(processor) {
		return ProcessorObjectKeys{}, errors.Newf("invalid processor %q", processor)
	}
	prefix += "/processor=" + processor
	return ProcessorObjectKeys{
		Processing: prefix + "/processing.json",
		Processed:  prefix + "/processed.json",
		Loaded:     prefix + "/loaded.json",
	}, nil
}

func ReclaimedKey(crawlID, selection string, part int) (string, error) {
	prefix, err := StatePartPrefix(crawlID, selection, part)
	if err != nil {
		return "", err
	}
	return prefix + "/reclaimed.json", nil
}
