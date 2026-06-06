package fixture

import (
	"encoding/json"
	"os"

	"github.com/cockroachdb/errors"
)

type Input struct {
	SourceLang string `json:"source_lang"`
	TargetLang string `json:"target_lang"`
	Items      []Item `json:"items"`
}

type Item struct {
	ID       string `json:"id"`
	Category string `json:"category"`
	Text     string `json:"text"`
}

func Load(path string) (Input, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return Input{}, errors.Wrap(err, "read translation input")
	}
	var input Input
	if err := json.Unmarshal(body, &input); err != nil {
		return Input{}, errors.Wrap(err, "decode translation input")
	}
	if len(input.Items) == 0 {
		return Input{}, errors.New("translation input contains no items")
	}
	return input, nil
}

func SelectItems(items []Item, limit int) []Item {
	if limit <= 0 || limit >= len(items) {
		return append([]Item(nil), items...)
	}
	return append([]Item(nil), items[:limit]...)
}

func ChunkItems(items []Item, size int) [][]Item {
	if size <= 0 {
		size = len(items)
	}
	chunks := make([][]Item, 0, (len(items)+size-1)/size)
	for index := 0; index < len(items); index += size {
		end := index + size
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, append([]Item(nil), items[index:end]...))
	}
	return chunks
}
