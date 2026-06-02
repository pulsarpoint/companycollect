package actions

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func TestClaimedDomainSearchRecordsExtractFactsAndDropRawPayload(t *testing.T) {
	name := "BORTIGARD AS"
	rows := []db.ClaimBrregWorkflowTaskSelectionBatchRow{
		{
			RawRecordID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			TaskAttemptID:      uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
			OrganizationNumber: "810202572",
			OrganizationName:   &name,
			RawPayload: []byte(`{
				"navn": "BORTIGARD AS",
				"aktivitet": ["Drive utleie av fast eiendom."],
				"forretningsadresse": {
					"adresse": ["Lokkeveien 18"],
					"postnummer": "3085",
					"poststed": "HOLMESTRAND",
					"landkode": "NO"
				},
				"naeringskode1": {"kode": "41.000", "beskrivelse": "Oppforing av bygninger"}
			}`),
			Attempt: 1,
		},
	}

	records := claimedDomainSearchRecordsFromRows(rows)

	require.Len(t, records, 1)
	require.Empty(t, records[0].RawPayload)
	require.Equal(t, "BORTIGARD AS", records[0].Facts.CompanyName)
	require.Equal(t, "NO", records[0].Facts.Country)
	require.Equal(t, []string{"Lokkeveien 18"}, records[0].Facts.AddressLines)
	require.Equal(t, "HOLMESTRAND", records[0].Facts.City)
	require.Equal(t, "3085", records[0].Facts.PostalCode)
	require.Equal(t, []string{"Drive utleie av fast eiendom."}, records[0].Facts.BusinessActivity)
	require.Equal(t, []string{"41.000 Oppforing av bygninger"}, records[0].Facts.IndustryCodes)
}
