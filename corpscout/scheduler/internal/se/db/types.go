package sedb

import (
	"encoding/json"

	"github.com/google/uuid"
)

const (
	jsonPayloadEmptyObject = "{}"
	jsonPayloadEmptyArray  = "[]"
)

type IngestRawRecordsResult struct {
	RowsSeen              int32
	RowsWritten           int32
	RowsInsertedNew       int32
	RowsExistingUnchanged int32
	RowsNewVersions       int32
	RawRecordIDs          []uuid.UUID
}

type RawRecord struct {
	SourceFileID        uuid.UUID
	SourceNativeID      string
	OrganizationNumber  string
	OrganizationName    string
	RegistrationStatus  string
	LegalForm           string
	BusinessDescription string
	SNICodes            []byte
	PostalAddress       []byte
	RawPayload          []byte
	PayloadHash         string
	RunID               string
	Metadata            []byte
}

type BolagsverketRawRecord struct {
	SourceFileID                                      uuid.UUID
	SourceRecordKey                                   string
	RowNumber                                         int32
	Organisationsidentitet                            string
	OrganizationNumber                                string
	Namnskyddslopnummer                               string
	Registreringsland                                 string
	Organisationsnamn                                 string
	OrganizationName                                  string
	Organisationsform                                 string
	Avregistreringsdatum                              string
	Avregistreringsorsak                              string
	PagandeAvvecklingsEllerOmstruktureringsforfarande string
	Registreringsdatum                                string
	Verksamhetsbeskrivning                            string
	Postadress                                        string
	PostalAddress                                     []byte
	RawPayload                                        []byte
	PayloadHash                                       string
	RunID                                             string
	Metadata                                          []byte
}

type SCBRawRecord struct {
	SourceFileID       uuid.UUID
	SourceRecordKey    string
	RowNumber          int32
	ForAndrTyp         string
	COAdress           string
	Foretagsnamn       string
	FtgStat            string
	Gatuadress         string
	JEStat             string
	JurForm            string
	Namn               string
	Ng1                string
	Ng2                string
	Ng3                string
	Ng4                string
	Ng5                string
	PeOrgNr            string
	OrganizationNumber string
	PostNr             string
	PostOrt            string
	RegDatKtid         string
	Reklamsparrtyp     string
	MCOAdress          string
	MForetagsnamn      string
	MFtgStat           string
	MGatuadress        string
	MJEStat            string
	MJurForm           string
	MNamn              string
	MNg1               string
	MNg2               string
	MNg3               string
	MNg4               string
	MNg5               string
	MPostNr            string
	MPostOrt           string
	MRegDatKtid        string
	MReklamsparrtyp    string
	MaskColumns        []byte
	SNICodes           []byte
	PostalAddress      []byte
	RawPayload         []byte
	PayloadHash        string
	RunID              string
	Metadata           []byte
}

type currentRawRecord struct {
	ID          uuid.UUID
	PayloadHash string
}

type ProcessedSourceFile struct {
	ID          uuid.UUID
	SourceURL   string
	PayloadHash string
	RowsSeen    int32
	RowsWritten int32
	Metadata    []byte
}

func jsonObject(value []byte) []byte {
	if len(value) == 0 || !json.Valid(value) {
		return []byte(jsonPayloadEmptyObject)
	}
	return value
}

func jsonArray(value []byte) []byte {
	if len(value) == 0 || !json.Valid(value) {
		return []byte(jsonPayloadEmptyArray)
	}
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		return []byte(jsonPayloadEmptyArray)
	}
	if _, ok := decoded.([]any); !ok {
		return []byte(jsonPayloadEmptyArray)
	}
	return value
}
