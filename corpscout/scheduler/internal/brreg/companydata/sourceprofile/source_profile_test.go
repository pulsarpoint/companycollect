package sourceprofile

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBuildBatchNormalizesRawRecord(t *testing.T) {
	rawRecordID := uuid.New()
	rawPayload := mustTestJSON(t, map[string]any{
		"navn":                          "BORTIGARD TEST AS",
		"konkurs":                       false,
		"erIKonsern":                    false,
		"underAvvikling":                false,
		"harRegistrertAntallAnsatte":    false,
		"registrertIMvaregisteret":      true,
		"registrertIForetaksregisteret": true,
		"organisasjonsform": map[string]any{
			"kode":        "AS",
			"beskrivelse": "Aksjeselskap",
		},
		"stiftelsesdato": "1975-06-04",
		"aktivitet": []string{
			"Drive utleie av fast eiendom.",
		},
		"vedtektsfestetFormaal": []string{
			"Drive utleie av fast eiendom.",
		},
		"forretningsadresse": map[string]any{
			"adresse":       []string{"Løkkeveien 18"},
			"postnummer":    "3085",
			"poststed":      "HOLMESTRAND",
			"kommune":       "HOLMESTRAND",
			"kommunenummer": "3903",
			"land":          "Norge",
			"landkode":      "NO",
		},
		"naeringskode1": map[string]any{
			"kode":        "41.000",
			"beskrivelse": "Oppføring av bygninger",
		},
		"telefon": "33051963",
		"kapital": map[string]any{
			"type":         "Aksjekapital",
			"belop":        81870,
			"valuta":       "NOK",
			"innfortDato":  "2012-07-09",
			"antallAksjer": 2283028025,
		},
	})

	batch, err := BuildBatch(Command{
		Trigger: "manual",
		Records: []RawRecord{{
			ID:                 rawRecordID,
			SourceNativeID:     "999888777",
			OrganizationNumber: "999888777",
			OrganizationName:   "BORTIGARD TEST AS",
			RegistrationStatus: "active",
			Website:            "bortigard.example",
			CountryISO2:        "NO",
			RawPayload:         rawPayload,
			PayloadHash:        "payload-hash",
		}},
	})
	require.NoError(t, err)
	require.Len(t, batch.Companies, 1)
	require.Equal(t, "BORTIGARD TEST AS", batch.Companies[0].OrganizationName)
	require.Equal(t, "active", batch.Companies[0].LifecycleStatus)
	require.Equal(t, "AS", batch.Companies[0].OrganizationFormCode)
	require.Equal(t, "Aksjeselskap", batch.Companies[0].OrganizationFormLabel)
	require.Contains(t, batch.Companies[0].ActivityDescription, "Drive utleie")

	require.Len(t, batch.Addresses, 1)
	require.Equal(t, "business", batch.Addresses[0].AddressType)
	require.Equal(t, []string{"Løkkeveien 18"}, batch.Addresses[0].StreetLines)
	require.Equal(t, "HOLMESTRAND", batch.Addresses[0].City)

	require.Len(t, batch.Industries, 1)
	require.Equal(t, "naeringskode1", batch.Industries[0].SourceField)
	require.Equal(t, "41.000", batch.Industries[0].SourceCode)
	require.Equal(t, "41.00", batch.Industries[0].MappedNACECode)
	require.Equal(t, "sn_level_5_to_nace_class", batch.Industries[0].MappingMethod)

	require.Len(t, batch.Websites, 1)
	require.Equal(t, "https://bortigard.example", batch.Websites[0].URL)
	require.Equal(t, "bortigard.example", batch.Websites[0].Host)

	require.Len(t, batch.Domains, 1)
	require.Equal(t, "bortigard.example", batch.Domains[0].NormalizedDomain)

	require.Len(t, batch.Contacts, 1)
	require.Equal(t, "phone", batch.Contacts[0].ContactType)
	require.Equal(t, "33051963", batch.Contacts[0].NormalizedValue)

	require.Len(t, batch.Capital, 1)
	require.Equal(t, "Aksjekapital", batch.Capital[0].CapitalType)
	require.Equal(t, "81870", batch.Capital[0].OriginalAmountText)
	require.Equal(t, "2283028025", batch.Capital[0].ShareCountText)
}

func mustTestJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return data
}
