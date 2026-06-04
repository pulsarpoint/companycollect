package bulk

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
