package bulk

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestStreamRecordsReadsSemicolonCSV(t *testing.T) {
	input := "registrikood;\u00e4rinimi;staatus;oiguslik_vorm;kmkr_nr;www;email;telefon\n" +
		"12345678;Example OU;R;Osa\u00fching;EE123456789;https://example.ee;info@example.ee;+372555555\n"

	var records []Record
	result, err := StreamRecords(context.Background(), bytes.NewBufferString(input), 0, func(record Record) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.Len(t, records, 1)
	require.Equal(t, "12345678", records[0].RegistryCode)
	require.Equal(t, "Example OU", records[0].LegalName)
	require.Equal(t, "Osa\u00fching", records[0].LegalForm)
	require.Equal(t, "EE123456789", records[0].VATNumber)
	require.Equal(t, "https://example.ee", records[0].Website)
	require.NotEmpty(t, records[0].PayloadHash)
	require.JSONEq(t, `{"email":"info@example.ee","kmkr_nr":"EE123456789","oiguslik_vorm":"Osa\u00fching","registrikood":"12345678","staatus":"R","telefon":"+372555555","www":"https://example.ee","\u00e4rinimi":"Example OU"}`, string(records[0].RawPayload))
}

func TestStreamRecordsReadsCSVFromZip(t *testing.T) {
	var zipBody bytes.Buffer
	zw := zip.NewWriter(&zipBody)
	w, err := zw.Create("ettevotjad.csv")
	require.NoError(t, err)
	_, err = w.Write([]byte("registrikood,nimi\n87654321,Zip Example AS\n"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	var records []Record
	result, err := StreamRecords(context.Background(), bytes.NewReader(zipBody.Bytes()), 0, func(record Record) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, "ettevotjad.csv", result.FileName)
	require.Len(t, records, 1)
	require.Equal(t, "87654321", records[0].RegistryCode)
	require.Equal(t, "Zip Example AS", records[0].LegalName)
}

func TestNewRecordReadsJSONPayload(t *testing.T) {
	raw := json.RawMessage(`{"registry_code":"99887766","legal_name":"JSON OU","status":"active","website":"https://json.example"}`)

	record, err := NewRecord(raw)

	require.NoError(t, err)
	require.Equal(t, "99887766", record.RegistryCode)
	require.Equal(t, "JSON OU", record.LegalName)
	require.Equal(t, "active", record.RegistrationStatus)
	require.Equal(t, "https://json.example", record.Website)
}

func TestNewRecordReadsGeneralDataJSONPayload(t *testing.T) {
	raw := json.RawMessage(`{
		"ariregistri_kood": 16752073,
		"nimi": "007 Agent & Partners OÜ",
		"kmkr_number": "EE123456789",
		"yldandmed": {
			"staatus": "R",
			"staatus_tekstina": "Registrisse kantud",
			"oiguslik_vorm": "OÜ",
			"oiguslik_vorm_tekstina": "Osaühing",
			"sidevahendid": [
				{"liik": "EMAIL", "sisu": "info@example.ee"},
				{"liik": "MOB", "sisu": "+372 5587811"},
				{"liik": "WWW", "sisu": "https://example.ee/"}
			]
		}
	}`)

	record, err := NewRecord(raw)

	require.NoError(t, err)
	require.Equal(t, "16752073", record.RegistryCode)
	require.Equal(t, "007 Agent & Partners OÜ", record.LegalName)
	require.Equal(t, "Registrisse kantud", record.RegistrationStatus)
	require.Equal(t, "Osaühing", record.LegalForm)
	require.Equal(t, "EE123456789", record.VATNumber)
	require.Equal(t, "https://example.ee/", record.Website)
	require.Equal(t, "info@example.ee", record.Email)
	require.Equal(t, "+372 5587811", record.Phone)
}

func TestDefaultSourceURLUsesGeneralDataJSON(t *testing.T) {
	require.Contains(t, DefaultSourceURL, "ettevotja_rekvisiidid__yldandmed.json.zip")
}

func TestRecordUpsertParamsUsesRegistryCodeAsNativeID(t *testing.T) {
	record := Record{
		RegistryCode: "12345678",
		LegalName:    "Params OU",
		RawPayload:   json.RawMessage(`{"registrikood":"12345678"}`),
		PayloadHash:  "hash",
	}

	params := record.UpsertParams(pgtype.UUID{}, pgtype.UUID{}, []byte(`{"source":"test"}`))

	require.Equal(t, "12345678", params.SourceNativeID)
	require.Equal(t, "12345678", params.RegistryCode)
	require.Equal(t, "Params OU", *params.LegalName)
	require.Equal(t, "EE", *params.CountryIso2)
	require.JSONEq(t, `{"source":"test"}`, string(params.Metadata))
}
