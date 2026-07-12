package partspec

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

func Parse(value string) ([]int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("parts are required")
	}
	lowText, highText, ranged := strings.Cut(value, "-")
	if !ranged {
		highText = lowText
	}
	low, err := strconv.Atoi(lowText)
	if err != nil || low < 0 {
		return nil, errors.Newf("invalid parts %q: expected a non-negative number or range N-M", value)
	}
	high, err := strconv.Atoi(highText)
	if err != nil || high < low {
		return nil, errors.Newf("invalid parts %q: expected an ascending range N-M", value)
	}
	if high-low > 10_000 {
		return nil, errors.Newf("invalid parts %q: range exceeds 10001 parts", value)
	}
	parts := make([]int, high-low+1)
	for index := range parts {
		parts[index] = low + index
	}
	return parts, nil
}

func Format(parts []int) string {
	if len(parts) == 0 {
		return ""
	}
	var formatted []string
	start := parts[0]
	end := start
	for _, part := range parts[1:] {
		if part == end+1 {
			end = part
			continue
		}
		formatted = append(formatted, formatRange(start, end))
		start = part
		end = part
	}
	return strings.Join(append(formatted, formatRange(start, end)), ",")
}

func formatRange(start, end int) string {
	if start == end {
		return strconv.Itoa(start)
	}
	return fmt.Sprintf("%d-%d", start, end)
}
