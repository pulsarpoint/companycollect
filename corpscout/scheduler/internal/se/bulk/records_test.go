package bulk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"
)

func TestStreamRecordsExtractsSwedenHVDOrganizationFields(t *testing.T) {
	raw := `{"data":[{
		"identitetsbeteckning":"556677-8899",
		"organisationsnamn":"Exempel Sverige AB",
		"organisationsform":{"kod":"AB","klartext":"Aktiebolag"},
		"avregistreradOrganisation":{"avregistrerad":false},
		"verksamhetsbeskrivning":"Konsultverksamhet inom IT",
		"näringsgrenOrganisation":[{"sni":{"kod":"62010","klartext":"Dataprogrammering"}}],
		"postadressOrganisation":{"postadress":{"postnummer":"11122","postort":"Stockholm"}}
	}]}`

	var records []Record
	result, err := StreamRecords(context.Background(), strings.NewReader(raw), 0, func(record Record) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.Len(t, records, 1)
	require.Equal(t, "5566778899", records[0].OrganizationNumber)
	require.Equal(t, "Exempel Sverige AB", records[0].OrganizationName)
	require.Equal(t, "active", records[0].RegistrationStatus)
	require.Equal(t, "Aktiebolag", records[0].LegalForm)
	require.JSONEq(t, `[{"code":"62010","label":"Dataprogrammering"}]`, string(records[0].SNICodes))
	require.JSONEq(t, `{"post_code":"11122","city":"Stockholm"}`, string(records[0].PostalAddress))
	require.NotEmpty(t, records[0].PayloadHash)
}

func TestStreamBolagsverketRecordsExtractsCSVRows(t *testing.T) {
	raw := strings.Join([]string{
		`organisationsidentitet;namnskyddslopnummer;registreringsland;organisationsnamn;organisationsform;avregistreringsdatum;avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;registreringsdatum;verksamhetsbeskrivning;postadress`,
		`"5566778899$ORGNR-IDORG";"1";"SE-LAND";"Exempel Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02";"AB-ORGFO";"";"";"|LI-AVOMFO$2026-04-01";"2020-01-02";"Konsultverksamhet inom IT";"Box 1$$STOCKHOLM$11122$SE-LAND"`,
		"",
	}, "\n")

	var records []BolagsverketRecord
	result, err := StreamBolagsverketRecords(context.Background(), strings.NewReader(raw), 0, func(record BolagsverketRecord) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.Len(t, records, 1)
	require.Equal(t, "5566778899", records[0].OrganizationNumber)
	require.Equal(t, "5566778899$ORGNR-IDORG", records[0].Organisationsidentitet)
	require.Equal(t, "Exempel Sverige AB", records[0].OrganizationName)
	require.Equal(t, "AB-ORGFO", records[0].Organisationsform)
	require.Equal(t, "2020-01-02", records[0].Registreringsdatum)
	require.Equal(t, "|LI-AVOMFO$2026-04-01", records[0].PagandeAvvecklingsEllerOmstruktureringsforfarande)
	require.JSONEq(t, `{"address_line_1":"Box 1","city":"STOCKHOLM","post_code":"11122","country_code":"SE-LAND"}`, string(records[0].PostalAddress))
	var payload map[string]string
	require.NoError(t, json.Unmarshal(records[0].RawPayload, &payload))
	require.Len(t, payload, 11)
	require.Equal(t, "5566778899$ORGNR-IDORG", payload["organisationsidentitet"])
	require.Equal(t, "AB-ORGFO", payload["organisationsform"])
	require.NotEmpty(t, records[0].PayloadHash)
}

func TestStreamBolagsverketRecordsAllowsBareQuotesInQuotedFields(t *testing.T) {
	raw := strings.Join([]string{
		`organisationsidentitet;namnskyddslopnummer;registreringsland;organisationsnamn;organisationsform;avregistreringsdatum;avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;registreringsdatum;verksamhetsbeskrivning;postadress`,
		`"2220000000$ORGNR-IDORG";"1";"SE-LAND";"Ensillre Väglyseförening "Ljuspunkten" med firma Ljuspunkten.$FORETAGSNAMN-ORGNAM$1985-10-01";"I-ORGFO";"2017-03-30";"OVERK-AVORG";"";"1985-10-01";"Anläggande av vägbelysning Ensillre - Hermanboda.";"Ensillre 2286$$ÅNGE$84100$SE-LAND"`,
	}, "\n")

	var records []BolagsverketRecord
	result, err := StreamBolagsverketRecords(context.Background(), strings.NewReader(raw), 0, func(record BolagsverketRecord) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.Len(t, records, 1)
	require.Equal(t, "2220000000", records[0].OrganizationNumber)
	require.Equal(t, `Ensillre Väglyseförening "Ljuspunkten" med firma Ljuspunkten.`, records[0].OrganizationName)
}

func TestStreamBolagsverketRecordsSanitizesNULBytes(t *testing.T) {
	raw := strings.Join([]string{
		`organisationsidentitet;namnskyddslopnummer;registreringsland;organisationsnamn;organisationsform;avregistreringsdatum;avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;registreringsdatum;verksamhetsbeskrivning;postadress`,
		"\"5566778899$ORGNR-IDORG\";\"1\";\"SE-LAND\";\"Exempel\x00 Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02\";\"AB-ORGFO\";\"\";\"\";\"\";\"2020-01-02\";\"Konsultverksamhet\";\"Box 1$$STOCKHOLM$11122$SE-LAND\"",
	}, "\n")

	var records []BolagsverketRecord
	result, err := StreamBolagsverketRecords(context.Background(), strings.NewReader(raw), 0, func(record BolagsverketRecord) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.Len(t, records, 1)
	require.Equal(t, "Exempel Sverige AB", records[0].OrganizationName)
	require.NotContains(t, records[0].Organisationsnamn, "\x00")
	require.NotContains(t, string(records[0].RawPayload), "\u0000")
	require.Equal(t, []RecordIssue{{
		Code:  "nul_bytes_removed",
		Field: "organisationsnamn",
		Count: 1,
	}}, records[0].Issues)
}

func TestStreamBolagsverketRecordsStopsReadingAtLimit(t *testing.T) {
	reader := &errorAfterChunksReader{chunks: []string{
		`organisationsidentitet;namnskyddslopnummer;registreringsland;organisationsnamn;organisationsform;avregistreringsdatum;avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;registreringsdatum;verksamhetsbeskrivning;postadress`,
		`"5566778899$ORGNR-IDORG";"1";"SE-LAND";"Exempel Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02";"AB-ORGFO";"";"";"";"2020-01-02";"Konsultverksamhet inom IT";"Box 1$$STOCKHOLM$11122$SE-LAND"`,
	}}

	var records []BolagsverketRecord
	result, err := StreamBolagsverketRecords(context.Background(), reader, 1, func(record BolagsverketRecord) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.Len(t, records, 1)
	require.Equal(t, "5566778899", records[0].OrganizationNumber)
}

func TestStreamBolagsverketRecordsSkipsAlreadySeenRows(t *testing.T) {
	raw := strings.Join([]string{
		`organisationsidentitet;namnskyddslopnummer;registreringsland;organisationsnamn;organisationsform;avregistreringsdatum;avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;registreringsdatum;verksamhetsbeskrivning;postadress`,
		`"1111111111$ORGNR-IDORG";"1";"SE-LAND";"First Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02";"AB-ORGFO";"";"";"";"2020-01-02";"";""`,
		`"2222222222$ORGNR-IDORG";"1";"SE-LAND";"Second Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02";"AB-ORGFO";"";"";"";"2020-01-02";"";""`,
		`"3333333333$ORGNR-IDORG";"1";"SE-LAND";"Third Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02";"AB-ORGFO";"";"";"";"2020-01-02";"";""`,
	}, "\n")

	var records []BolagsverketRecord
	result, err := StreamBolagsverketRecordsFromOffset(context.Background(), strings.NewReader(raw), 3, 2, func(record BolagsverketRecord) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 3, result.RowsSeen)
	require.Len(t, records, 1)
	require.Equal(t, "3333333333", records[0].OrganizationNumber)
	require.EqualValues(t, 3, records[0].RowNumber)
}

func TestStreamSCBRecordsExtractsISO88591TabRows(t *testing.T) {
	raw := strings.Join([]string{
		"ForAndrTyp\tCOAdress\tForetagsnamn\tFtgStat\tGatuadress\tJEStat\tJurForm\tNamn\tNg1\tNg2\tNg3\tNg4\tNg5\tPeOrgNr\tPostNr\tPostOrt\tRegDatKtid\tReklamsparrtyp\tmCOAdress\tmForetagsnamn\tmFtgStat\tmGatuadress\tmJEStat\tmJurForm\tmNamn\tmNg1\tmNg2\tmNg3\tmNg4\tmNg5\tmPostNr\tmPostOrt\tmRegDatKtid\tmReklamsparrtyp\t",
		"1\tÅSA TEST\t\t1\tSTORGATAN 1\t1\t49\tEXEMPEL SVERIGE AB\t62010\t70220\t\t\t\t165566778899\t11122\tSTOCKHOLM\t20200102\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t",
		"",
	}, "\n")
	encoded, err := charmap.ISO8859_1.NewEncoder().Bytes([]byte(raw))
	require.NoError(t, err)

	var records []SCBRecord
	result, err := StreamSCBRecords(context.Background(), bytes.NewReader(encoded), 0, func(record SCBRecord) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.Len(t, records, 1)
	require.Equal(t, "165566778899", records[0].PeOrgNr)
	require.Equal(t, "5566778899", records[0].OrganizationNumber)
	require.Equal(t, "EXEMPEL SVERIGE AB", records[0].Namn)
	require.Equal(t, "ÅSA TEST", records[0].COAdress)
	require.Equal(t, "49", records[0].JurForm)
	require.JSONEq(t, `[{"code":"62010","position":1},{"code":"70220","position":2}]`, string(records[0].SNICodes))
	require.JSONEq(t, `{"care_of":"ÅSA TEST","street_address":"STORGATAN 1","post_code":"11122","city":"STOCKHOLM"}`, string(records[0].PostalAddress))
	var payload map[string]string
	require.NoError(t, json.Unmarshal(records[0].RawPayload, &payload))
	require.Len(t, payload, 34)
	require.Equal(t, "165566778899", payload["PeOrgNr"])
	require.Equal(t, "EXEMPEL SVERIGE AB", payload["Namn"])
	require.Equal(t, "49", payload["JurForm"])
	require.NotEmpty(t, records[0].PayloadHash)
}

type errorAfterChunksReader struct {
	chunks []string
	index  int
}

func (r *errorAfterChunksReader) Read(p []byte) (int, error) {
	if r.index >= len(r.chunks) {
		return 0, errors.New("read after expected limit")
	}
	chunk := r.chunks[r.index] + "\n"
	r.index++
	if len(chunk) > len(p) {
		return 0, errors.New("test chunk does not fit read buffer")
	}
	copy(p, chunk)
	return len(chunk), nil
}

var _ io.Reader = (*errorAfterChunksReader)(nil)
