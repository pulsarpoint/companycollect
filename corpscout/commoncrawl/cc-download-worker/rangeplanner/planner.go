package rangeplanner

import (
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

type Record struct {
	ID       int64
	WARCFile string
	Offset   int64
	Length   int64
}

type Policy struct {
	Name           string
	MaxGapBytes    int64
	MaxRangeBytes  int64
	MaxJunkPercent float64
}

type Range struct {
	WARCFile      string
	Start         int64
	Length        int64
	SelectedBytes int64
	RecordIDs     []int64
}

func Plan(records []Record, policy Policy) ([]Range, error) {
	if err := validatePolicy(policy); err != nil {
		return nil, err
	}
	byWARC := make(map[string][]Record)
	for _, record := range records {
		if err := validateRecord(record); err != nil {
			return nil, err
		}
		byWARC[record.WARCFile] = append(byWARC[record.WARCFile], record)
	}
	filenames := make([]string, 0, len(byWARC))
	for filename := range byWARC {
		filenames = append(filenames, filename)
	}
	sort.Strings(filenames)

	planned := make([]Range, 0, len(records))
	for _, filename := range filenames {
		objectRecords := byWARC[filename]
		sort.Slice(objectRecords, func(left, right int) bool {
			if objectRecords[left].Offset == objectRecords[right].Offset {
				return objectRecords[left].ID < objectRecords[right].ID
			}
			return objectRecords[left].Offset < objectRecords[right].Offset
		})
		planned = append(planned, planObject(filename, objectRecords, policy)...)
	}
	return planned, nil
}

func planObject(filename string, records []Record, policy Policy) []Range {
	if len(records) == 0 {
		return nil
	}
	currentEnd := records[0].Offset + records[0].Length
	current := Range{
		WARCFile:      filename,
		Start:         records[0].Offset,
		Length:        records[0].Length,
		SelectedBytes: records[0].Length,
		RecordIDs:     []int64{records[0].ID},
	}
	planned := make([]Range, 0, len(records))
	for _, record := range records[1:] {
		recordEnd := record.Offset + record.Length
		candidateEnd := max(currentEnd, recordEnd)
		candidateLength := candidateEnd - current.Start
		additionalSelectedBytes := max(int64(0), recordEnd-max(record.Offset, currentEnd))
		candidateSelectedBytes := current.SelectedBytes + additionalSelectedBytes
		gap := max(int64(0), record.Offset-currentEnd)
		junkPercent := 100 * float64(candidateLength-candidateSelectedBytes) / float64(candidateLength)
		if gap <= policy.MaxGapBytes && candidateLength <= policy.MaxRangeBytes && junkPercent <= policy.MaxJunkPercent {
			currentEnd = candidateEnd
			current.Length = candidateLength
			current.SelectedBytes = candidateSelectedBytes
			current.RecordIDs = append(current.RecordIDs, record.ID)
			continue
		}
		planned = append(planned, current)
		currentEnd = recordEnd
		current = Range{
			WARCFile:      filename,
			Start:         record.Offset,
			Length:        record.Length,
			SelectedBytes: record.Length,
			RecordIDs:     []int64{record.ID},
		}
	}
	return append(planned, current)
}

func validatePolicy(policy Policy) error {
	if strings.TrimSpace(policy.Name) == "" {
		return errors.New("range policy name is required")
	}
	if policy.MaxGapBytes < 0 || policy.MaxRangeBytes <= 0 {
		return errors.Newf("invalid range policy gap=%d range=%d", policy.MaxGapBytes, policy.MaxRangeBytes)
	}
	if math.IsNaN(policy.MaxJunkPercent) || math.IsInf(policy.MaxJunkPercent, 0) || policy.MaxJunkPercent < 0 || policy.MaxJunkPercent > 100 {
		return errors.Newf("maximum junk percentage must be between 0 and 100, got %f", policy.MaxJunkPercent)
	}
	return nil
}

func validateRecord(record Record) error {
	if strings.TrimSpace(record.WARCFile) == "" || record.Offset < 0 || record.Length <= 0 {
		return errors.Newf("invalid record id=%d WARC=%q offset=%d length=%d", record.ID, record.WARCFile, record.Offset, record.Length)
	}
	if record.Length > math.MaxInt64-record.Offset {
		return errors.Newf("record range overflows id=%d offset=%d length=%d", record.ID, record.Offset, record.Length)
	}
	return nil
}
