package bulk

import (
	"bytes"
	"compress/gzip"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamRecordsReadsGzippedEmbeddedEnvelopeAndHonorsLimit(t *testing.T) {
	body := gzipBytes(t, []byte(`{
		"_embedded": {
			"enheter": [
				{"organisasjonsnummer":"810202572","navn":"BORTIGARD AS","hjemmeside":"https://bortigard.no"},
				{"organisasjonsnummer":"811111111","navn":"SECOND AS"}
			]
		}
	}`))

	var records []Record
	result, err := StreamRecords(context.Background(), bytes.NewReader(body), 1, func(record Record) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.Len(t, records, 1)
	require.Equal(t, "810202572", records[0].OrganizationNumber)
	require.Equal(t, "BORTIGARD AS", records[0].OrganizationName)
	require.Equal(t, "https://bortigard.no", records[0].Website)
	require.NotEmpty(t, records[0].PayloadHash)
	require.JSONEq(t, `{"organisasjonsnummer":"810202572","navn":"BORTIGARD AS","hjemmeside":"https://bortigard.no"}`, string(records[0].RawPayload))
}

func TestStreamRecordsReadsTopLevelArray(t *testing.T) {
	var records []Record
	result, err := StreamRecords(context.Background(), bytes.NewBufferString(`[
		{"organisasjonsnummer":"810202572","navn":"BORTIGARD AS"},
		{"organisasjonsnummer":"811111111","navn":"SECOND AS"}
	]`), 0, func(record Record) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 2, result.RowsSeen)
	require.Len(t, records, 2)
	require.Equal(t, "811111111", records[1].OrganizationNumber)
}

func TestStreamRecordsRejectsRecordWithoutOrganizationNumber(t *testing.T) {
	_, err := StreamRecords(context.Background(), bytes.NewBufferString(`[
		{"navn":"MISSING ORG"}
	]`), 0, func(Record) error {
		return nil
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "organization number is required")
}

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, err := writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}
