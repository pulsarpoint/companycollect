package bulk

import (
	"context"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestStreamLegalUnitsFileParsesSireneParquetRows(t *testing.T) {
	path := t.TempDir() + "/StockUniteLegale.parquet"
	err := parquet.WriteFile(path, []legalUnitParquetRow{
		{
			Siren:                                     strPtr("552100554"),
			StatutDiffusionUniteLegale:                strPtr("O"),
			UnitePurgeeUniteLegale:                    boolPtr(false),
			DateCreationUniteLegale:                   dateDaysPtr("1955-01-01"),
			SigleUniteLegale:                          strPtr("PP"),
			TrancheEffectifsUniteLegale:               strPtr("22"),
			AnneeEffectifsUniteLegale:                 int64Ptr(2024),
			DateDernierTraitementUniteLegale:          timePtr("2026-06-01T00:00:00Z"),
			NombrePeriodesUniteLegale:                 int64Ptr(3),
			CategorieEntreprise:                       strPtr("GE"),
			AnneeCategorieEntreprise:                  int64Ptr(2023),
			DateDebut:                                 dateDaysPtr("2024-01-01"),
			EtatAdministratifUniteLegale:              strPtr("A"),
			DenominationUniteLegale:                   strPtr("PULSAR POINT FRANCE"),
			CategorieJuridiqueUniteLegale:             int64Ptr(5710),
			ActivitePrincipaleUniteLegale:             strPtr("62.01Z"),
			NomenclatureActivitePrincipaleUniteLegale: strPtr("NAFRev2"),
			NicSiegeUniteLegale:                       strPtr("00042"),
			EconomieSocialeSolidaireUniteLegale:       strPtr("N"),
			SocieteMissionUniteLegale:                 strPtr("true"),
			CaractereEmployeurUniteLegale:             strPtr("O"),
			ActivitePrincipaleNAF25UniteLegale:        strPtr("62.10"),
		},
	})
	require.NoError(t, err)

	var records []LegalUnitRecord
	result, err := StreamLegalUnitsFile(context.Background(), path, 0, func(record LegalUnitRecord) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, "552100554", record.Siren)
	require.Equal(t, "PULSAR POINT FRANCE", record.LegalName)
	require.Equal(t, "5710", record.LegalFormCode)
	require.Equal(t, "62.01Z", record.PrimaryActivityCode)
	require.Equal(t, "00042", record.HeadquartersNIC)
	require.Equal(t, boolPtr(false), record.IsPurged)
	require.Equal(t, boolPtr(false), record.SocialSolidarity)
	require.Equal(t, boolPtr(true), record.MissionCompany)
	require.Equal(t, boolPtr(true), record.IsEmployer)
	require.Equal(t, datePtr("1955-01-01"), record.CreatedDate)
	require.Equal(t, timePtr("2026-06-01T00:00:00Z"), record.SourceUpdatedAt)
	require.NotEmpty(t, record.RawPayload)
	require.Len(t, record.PayloadHash, 64)
}

func TestStreamEstablishmentsFileParsesSireneParquetRows(t *testing.T) {
	path := t.TempDir() + "/StockEtablissement.parquet"
	err := parquet.WriteFile(path, []establishmentParquetRow{
		{
			Siren:                                       strPtr("552100554"),
			Nic:                                         strPtr("00042"),
			Siret:                                       strPtr("55210055400042"),
			StatutDiffusionEtablissement:                strPtr("O"),
			DateCreationEtablissement:                   dateDaysPtr("1955-01-01"),
			TrancheEffectifsEtablissement:               strPtr("22"),
			AnneeEffectifsEtablissement:                 int64Ptr(2024),
			DateDernierTraitementEtablissement:          timePtr("2026-06-01T00:00:00Z"),
			EtablissementSiege:                          boolPtr(true),
			NombrePeriodesEtablissement:                 int64Ptr(2),
			ComplementAdresseEtablissement:              strPtr("BAT A"),
			NumeroVoieEtablissement:                     strPtr("10"),
			TypeVoieEtablissement:                       strPtr("RUE"),
			LibelleVoieEtablissement:                    strPtr("DE PARIS"),
			CodePostalEtablissement:                     strPtr("75001"),
			LibelleCommuneEtablissement:                 strPtr("PARIS"),
			CodeCommuneEtablissement:                    strPtr("75101"),
			DateDebut:                                   dateDaysPtr("2024-01-01"),
			EtatAdministratifEtablissement:              strPtr("A"),
			Enseigne1Etablissement:                      strPtr("PULSAR PARIS"),
			ActivitePrincipaleEtablissement:             strPtr("62.01Z"),
			NomenclatureActivitePrincipaleEtablissement: strPtr("NAFRev2"),
			CaractereEmployeurEtablissement:             strPtr("O"),
			ActivitePrincipaleNAF25Etablissement:        strPtr("62.10"),
		},
	})
	require.NoError(t, err)

	var records []EstablishmentRecord
	result, err := StreamEstablishmentsFile(context.Background(), path, 0, func(record EstablishmentRecord) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, "552100554", record.Siren)
	require.Equal(t, "00042", record.NIC)
	require.Equal(t, "55210055400042", record.Siret)
	require.Equal(t, "PULSAR PARIS", record.TradeName1)
	require.Equal(t, "75001", record.PostalCode)
	require.Equal(t, "PARIS", record.CityLabel)
	require.Equal(t, boolPtr(true), record.IsHeadquarters)
	require.Equal(t, boolPtr(true), record.IsEmployer)
	require.Equal(t, datePtr("1955-01-01"), record.CreatedDate)
	require.Equal(t, timePtr("2026-06-01T00:00:00Z"), record.SourceUpdatedAt)
	require.NotEmpty(t, record.RawPayload)
	require.Len(t, record.PayloadHash, 64)
}

func strPtr(value string) *string {
	return &value
}

func boolPtr(value bool) *bool {
	return &value
}

func int64Ptr(value int64) *int64 {
	return &value
}

func dateDaysPtr(value string) *int32 {
	parsed := datePtr(value)
	days := int32(parsed.Sub(parquetDateEpoch).Hours() / 24)
	return &days
}

func datePtr(value string) *time.Time {
	parsed, err := time.Parse(time.DateOnly, value)
	if err != nil {
		panic(err)
	}
	return &parsed
}

func timePtr(value string) *time.Time {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return &parsed
}
