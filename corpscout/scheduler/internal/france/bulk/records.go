package bulk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/parquet-go/parquet-go"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	LegalUnitsDatasetKey           = "stock_unite_legale"
	EstablishmentsDatasetKey       = "stock_etablissement"
	LegalUnitsResourceID           = "350182c9-148a-46e0-8389-76c2ec1374a3"
	EstablishmentsResourceID       = "a29c1297-1f92-4e2a-8f6b-8c902ce96c5f"
	DefaultLegalUnitsSourceURL     = "https://www.data.gouv.fr/api/1/datasets/r/350182c9-148a-46e0-8389-76c2ec1374a3"
	DefaultEstablishmentsSourceURL = "https://www.data.gouv.fr/api/1/datasets/r/a29c1297-1f92-4e2a-8f6b-8c902ce96c5f"
	defaultParquetReadBatchSize    = 512
)

var parquetDateEpoch = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)

type StreamResult struct {
	RowsSeen int32
}

type LegalUnitRecord struct {
	Siren                       string
	DiffusionStatus             string
	IsPurged                    *bool
	CreatedDate                 *time.Time
	Acronym                     string
	Gender                      string
	FirstName1                  string
	FirstName2                  string
	FirstName3                  string
	FirstName4                  string
	UsualFirstName              string
	Pseudonym                   string
	AssociationIdentifier       string
	EmployeeBand                string
	EmployeeYear                string
	SourceUpdatedAt             *time.Time
	PeriodCount                 *int32
	CompanyCategory             string
	CompanyCategoryYear         string
	PeriodStartedAt             *time.Time
	AdministrativeStatus        string
	BirthName                   string
	UsageName                   string
	LegalName                   string
	UsualName1                  string
	UsualName2                  string
	UsualName3                  string
	LegalFormCode               string
	PrimaryActivityCode         string
	PrimaryActivityNomenclature string
	HeadquartersNIC             string
	SocialSolidarity            *bool
	MissionCompany              *bool
	IsEmployer                  *bool
	PrimaryActivityNAF25Code    string
	RawPayload                  json.RawMessage
	PayloadHash                 string
}

type EstablishmentRecord struct {
	Siren                       string
	NIC                         string
	Siret                       string
	DiffusionStatus             string
	CreatedDate                 *time.Time
	EmployeeBand                string
	EmployeeYear                string
	CraftsRegistryActivity      string
	SourceUpdatedAt             *time.Time
	IsHeadquarters              *bool
	PeriodCount                 *int32
	AddressComplement           string
	StreetNumber                string
	StreetNumberSuffix          string
	LastStreetNumber            string
	LastStreetNumberSuffix      string
	StreetType                  string
	StreetLabel                 string
	PostalCode                  string
	CityLabel                   string
	ForeignCityLabel            string
	SpecialDistribution         string
	CommuneCode                 string
	CedexCode                   string
	CedexLabel                  string
	ForeignCountryCode          string
	ForeignCountryLabel         string
	AddressIdentifier           string
	LambertX                    string
	LambertY                    string
	AddressComplement2          string
	StreetNumber2               string
	StreetNumberSuffix2         string
	StreetType2                 string
	StreetLabel2                string
	PostalCode2                 string
	CityLabel2                  string
	ForeignCityLabel2           string
	SpecialDistribution2        string
	CommuneCode2                string
	CedexCode2                  string
	CedexLabel2                 string
	ForeignCountryCode2         string
	ForeignCountryLabel2        string
	PeriodStartedAt             *time.Time
	AdministrativeStatus        string
	TradeName1                  string
	TradeName2                  string
	TradeName3                  string
	UsualName                   string
	PrimaryActivityCode         string
	PrimaryActivityNomenclature string
	IsEmployer                  *bool
	PrimaryActivityNAF25Code    string
	RawPayload                  json.RawMessage
	PayloadHash                 string
}

type legalUnitParquetRow struct {
	Siren                                     *string    `parquet:"siren,optional"`
	StatutDiffusionUniteLegale                *string    `parquet:"statutDiffusionUniteLegale,optional"`
	UnitePurgeeUniteLegale                    *bool      `parquet:"unitePurgeeUniteLegale,optional"`
	DateCreationUniteLegale                   *int32     `parquet:"dateCreationUniteLegale,optional,date"`
	SigleUniteLegale                          *string    `parquet:"sigleUniteLegale,optional"`
	SexeUniteLegale                           *string    `parquet:"sexeUniteLegale,optional"`
	Prenom1UniteLegale                        *string    `parquet:"prenom1UniteLegale,optional"`
	Prenom2UniteLegale                        *string    `parquet:"prenom2UniteLegale,optional"`
	Prenom3UniteLegale                        *string    `parquet:"prenom3UniteLegale,optional"`
	Prenom4UniteLegale                        *string    `parquet:"prenom4UniteLegale,optional"`
	PrenomUsuelUniteLegale                    *string    `parquet:"prenomUsuelUniteLegale,optional"`
	PseudonymeUniteLegale                     *string    `parquet:"pseudonymeUniteLegale,optional"`
	IdentifiantAssociationUniteLegale         *string    `parquet:"identifiantAssociationUniteLegale,optional"`
	TrancheEffectifsUniteLegale               *string    `parquet:"trancheEffectifsUniteLegale,optional"`
	AnneeEffectifsUniteLegale                 *int64     `parquet:"anneeEffectifsUniteLegale,optional"`
	DateDernierTraitementUniteLegale          *time.Time `parquet:"dateDernierTraitementUniteLegale,optional,timestamp(microsecond)"`
	NombrePeriodesUniteLegale                 *int64     `parquet:"nombrePeriodesUniteLegale,optional"`
	CategorieEntreprise                       *string    `parquet:"categorieEntreprise,optional"`
	AnneeCategorieEntreprise                  *int64     `parquet:"anneeCategorieEntreprise,optional"`
	DateDebut                                 *int32     `parquet:"dateDebut,optional,date"`
	EtatAdministratifUniteLegale              *string    `parquet:"etatAdministratifUniteLegale,optional"`
	NomUniteLegale                            *string    `parquet:"nomUniteLegale,optional"`
	NomUsageUniteLegale                       *string    `parquet:"nomUsageUniteLegale,optional"`
	DenominationUniteLegale                   *string    `parquet:"denominationUniteLegale,optional"`
	DenominationUsuelle1UniteLegale           *string    `parquet:"denominationUsuelle1UniteLegale,optional"`
	DenominationUsuelle2UniteLegale           *string    `parquet:"denominationUsuelle2UniteLegale,optional"`
	DenominationUsuelle3UniteLegale           *string    `parquet:"denominationUsuelle3UniteLegale,optional"`
	CategorieJuridiqueUniteLegale             *int64     `parquet:"categorieJuridiqueUniteLegale,optional"`
	ActivitePrincipaleUniteLegale             *string    `parquet:"activitePrincipaleUniteLegale,optional"`
	NomenclatureActivitePrincipaleUniteLegale *string    `parquet:"nomenclatureActivitePrincipaleUniteLegale,optional"`
	NicSiegeUniteLegale                       *string    `parquet:"nicSiegeUniteLegale,optional"`
	EconomieSocialeSolidaireUniteLegale       *string    `parquet:"economieSocialeSolidaireUniteLegale,optional"`
	SocieteMissionUniteLegale                 *string    `parquet:"societeMissionUniteLegale,optional"`
	CaractereEmployeurUniteLegale             *string    `parquet:"caractereEmployeurUniteLegale,optional"`
	ActivitePrincipaleNAF25UniteLegale        *string    `parquet:"activitePrincipaleNAF25UniteLegale,optional"`
}

type establishmentParquetRow struct {
	Siren                                          *string    `parquet:"siren,optional"`
	Nic                                            *string    `parquet:"nic,optional"`
	Siret                                          *string    `parquet:"siret,optional"`
	StatutDiffusionEtablissement                   *string    `parquet:"statutDiffusionEtablissement,optional"`
	DateCreationEtablissement                      *int32     `parquet:"dateCreationEtablissement,optional,date"`
	TrancheEffectifsEtablissement                  *string    `parquet:"trancheEffectifsEtablissement,optional"`
	AnneeEffectifsEtablissement                    *int64     `parquet:"anneeEffectifsEtablissement,optional"`
	ActivitePrincipaleRegistreMetiersEtablissement *string    `parquet:"activitePrincipaleRegistreMetiersEtablissement,optional"`
	DateDernierTraitementEtablissement             *time.Time `parquet:"dateDernierTraitementEtablissement,optional,timestamp(microsecond)"`
	EtablissementSiege                             *bool      `parquet:"etablissementSiege,optional"`
	NombrePeriodesEtablissement                    *int64     `parquet:"nombrePeriodesEtablissement,optional"`
	ComplementAdresseEtablissement                 *string    `parquet:"complementAdresseEtablissement,optional"`
	NumeroVoieEtablissement                        *string    `parquet:"numeroVoieEtablissement,optional"`
	IndiceRepetitionEtablissement                  *string    `parquet:"indiceRepetitionEtablissement,optional"`
	DernierNumeroVoieEtablissement                 *string    `parquet:"dernierNumeroVoieEtablissement,optional"`
	IndiceRepetitionDernierNumeroVoieEtablissement *string    `parquet:"indiceRepetitionDernierNumeroVoieEtablissement,optional"`
	TypeVoieEtablissement                          *string    `parquet:"typeVoieEtablissement,optional"`
	LibelleVoieEtablissement                       *string    `parquet:"libelleVoieEtablissement,optional"`
	CodePostalEtablissement                        *string    `parquet:"codePostalEtablissement,optional"`
	LibelleCommuneEtablissement                    *string    `parquet:"libelleCommuneEtablissement,optional"`
	LibelleCommuneEtrangerEtablissement            *string    `parquet:"libelleCommuneEtrangerEtablissement,optional"`
	DistributionSpecialeEtablissement              *string    `parquet:"distributionSpecialeEtablissement,optional"`
	CodeCommuneEtablissement                       *string    `parquet:"codeCommuneEtablissement,optional"`
	CodeCedexEtablissement                         *string    `parquet:"codeCedexEtablissement,optional"`
	LibelleCedexEtablissement                      *string    `parquet:"libelleCedexEtablissement,optional"`
	CodePaysEtrangerEtablissement                  *string    `parquet:"codePaysEtrangerEtablissement,optional"`
	LibellePaysEtrangerEtablissement               *string    `parquet:"libellePaysEtrangerEtablissement,optional"`
	IdentifiantAdresseEtablissement                *string    `parquet:"identifiantAdresseEtablissement,optional"`
	CoordonneeLambertAbscisseEtablissement         *string    `parquet:"coordonneeLambertAbscisseEtablissement,optional"`
	CoordonneeLambertOrdonneeEtablissement         *string    `parquet:"coordonneeLambertOrdonneeEtablissement,optional"`
	ComplementAdresse2Etablissement                *string    `parquet:"complementAdresse2Etablissement,optional"`
	NumeroVoie2Etablissement                       *string    `parquet:"numeroVoie2Etablissement,optional"`
	IndiceRepetition2Etablissement                 *string    `parquet:"indiceRepetition2Etablissement,optional"`
	TypeVoie2Etablissement                         *string    `parquet:"typeVoie2Etablissement,optional"`
	LibelleVoie2Etablissement                      *string    `parquet:"libelleVoie2Etablissement,optional"`
	CodePostal2Etablissement                       *string    `parquet:"codePostal2Etablissement,optional"`
	LibelleCommune2Etablissement                   *string    `parquet:"libelleCommune2Etablissement,optional"`
	LibelleCommuneEtranger2Etablissement           *string    `parquet:"libelleCommuneEtranger2Etablissement,optional"`
	DistributionSpeciale2Etablissement             *string    `parquet:"distributionSpeciale2Etablissement,optional"`
	CodeCommune2Etablissement                      *string    `parquet:"codeCommune2Etablissement,optional"`
	CodeCedex2Etablissement                        *string    `parquet:"codeCedex2Etablissement,optional"`
	LibelleCedex2Etablissement                     *string    `parquet:"libelleCedex2Etablissement,optional"`
	CodePaysEtranger2Etablissement                 *string    `parquet:"codePaysEtranger2Etablissement,optional"`
	LibellePaysEtranger2Etablissement              *string    `parquet:"libellePaysEtranger2Etablissement,optional"`
	DateDebut                                      *int32     `parquet:"dateDebut,optional,date"`
	EtatAdministratifEtablissement                 *string    `parquet:"etatAdministratifEtablissement,optional"`
	Enseigne1Etablissement                         *string    `parquet:"enseigne1Etablissement,optional"`
	Enseigne2Etablissement                         *string    `parquet:"enseigne2Etablissement,optional"`
	Enseigne3Etablissement                         *string    `parquet:"enseigne3Etablissement,optional"`
	DenominationUsuelleEtablissement               *string    `parquet:"denominationUsuelleEtablissement,optional"`
	ActivitePrincipaleEtablissement                *string    `parquet:"activitePrincipaleEtablissement,optional"`
	NomenclatureActivitePrincipaleEtablissement    *string    `parquet:"nomenclatureActivitePrincipaleEtablissement,optional"`
	CaractereEmployeurEtablissement                *string    `parquet:"caractereEmployeurEtablissement,optional"`
	ActivitePrincipaleNAF25Etablissement           *string    `parquet:"activitePrincipaleNAF25Etablissement,optional"`
}

func StreamLegalUnitsFile(ctx context.Context, path string, limit int32, emit func(LegalUnitRecord) error) (StreamResult, error) {
	if emit == nil {
		return StreamResult{}, errors.New("emit callback is required")
	}
	return streamParquetFile(ctx, path, limit, legalUnitRecordFromRow, emit)
}

func StreamEstablishmentsFile(ctx context.Context, path string, limit int32, emit func(EstablishmentRecord) error) (StreamResult, error) {
	if emit == nil {
		return StreamResult{}, errors.New("emit callback is required")
	}
	return streamParquetFile(ctx, path, limit, establishmentRecordFromRow, emit)
}

func streamParquetFile[T any, R any](
	ctx context.Context,
	path string,
	limit int32,
	convert func(T) (R, error),
	emit func(R) error,
) (StreamResult, error) {
	file, err := os.Open(path)
	if err != nil {
		return StreamResult{}, errors.Wrap(err, "open france sirene parquet file")
	}
	defer file.Close()

	reader, err := newParquetReader[T](file)
	if err != nil {
		return StreamResult{}, err
	}
	defer reader.Close()

	rows := make([]T, defaultParquetReadBatchSize)
	var result StreamResult
	for {
		if err := ctx.Err(); err != nil {
			return result, errors.Wrap(err, "stream france sirene parquet records")
		}
		if limit > 0 && result.RowsSeen >= limit {
			return result, nil
		}
		requested := len(rows)
		if limit > 0 {
			remaining := int(limit - result.RowsSeen)
			if remaining < requested {
				requested = remaining
			}
		}
		n, readErr := reader.Read(rows[:requested])
		for i := 0; i < n; i++ {
			record, err := convert(rows[i])
			if err != nil {
				return result, err
			}
			if err := emit(record); err != nil {
				return result, err
			}
			result.RowsSeen++
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return result, nil
			}
			return result, errors.Wrap(readErr, "read france sirene parquet rows")
		}
	}
}

func newParquetReader[T any](file *os.File) (reader *parquet.GenericReader[T], err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Newf("open france sirene parquet reader: %v", recovered)
		}
	}()
	return parquet.NewGenericReader[T](file), nil
}

func legalUnitRecordFromRow(row legalUnitParquetRow) (LegalUnitRecord, error) {
	siren := cleanDigits(value(row.Siren))
	if siren == "" {
		return LegalUnitRecord{}, errors.New("siren is required")
	}
	raw, hash, err := canonicalPayload(legalUnitRawPayload(row))
	if err != nil {
		return LegalUnitRecord{}, err
	}
	return LegalUnitRecord{
		Siren:                       siren,
		DiffusionStatus:             value(row.StatutDiffusionUniteLegale),
		IsPurged:                    boolValue(row.UnitePurgeeUniteLegale),
		CreatedDate:                 dateValue(row.DateCreationUniteLegale),
		Acronym:                     value(row.SigleUniteLegale),
		Gender:                      value(row.SexeUniteLegale),
		FirstName1:                  value(row.Prenom1UniteLegale),
		FirstName2:                  value(row.Prenom2UniteLegale),
		FirstName3:                  value(row.Prenom3UniteLegale),
		FirstName4:                  value(row.Prenom4UniteLegale),
		UsualFirstName:              value(row.PrenomUsuelUniteLegale),
		Pseudonym:                   value(row.PseudonymeUniteLegale),
		AssociationIdentifier:       value(row.IdentifiantAssociationUniteLegale),
		EmployeeBand:                value(row.TrancheEffectifsUniteLegale),
		EmployeeYear:                int64String(row.AnneeEffectifsUniteLegale),
		SourceUpdatedAt:             timeValue(row.DateDernierTraitementUniteLegale),
		PeriodCount:                 int64ToInt32(row.NombrePeriodesUniteLegale),
		CompanyCategory:             value(row.CategorieEntreprise),
		CompanyCategoryYear:         int64String(row.AnneeCategorieEntreprise),
		PeriodStartedAt:             dateValue(row.DateDebut),
		AdministrativeStatus:        value(row.EtatAdministratifUniteLegale),
		BirthName:                   value(row.NomUniteLegale),
		UsageName:                   value(row.NomUsageUniteLegale),
		LegalName:                   value(row.DenominationUniteLegale),
		UsualName1:                  value(row.DenominationUsuelle1UniteLegale),
		UsualName2:                  value(row.DenominationUsuelle2UniteLegale),
		UsualName3:                  value(row.DenominationUsuelle3UniteLegale),
		LegalFormCode:               int64String(row.CategorieJuridiqueUniteLegale),
		PrimaryActivityCode:         value(row.ActivitePrincipaleUniteLegale),
		PrimaryActivityNomenclature: value(row.NomenclatureActivitePrincipaleUniteLegale),
		HeadquartersNIC:             cleanDigits(value(row.NicSiegeUniteLegale)),
		SocialSolidarity:            parseOptionalBool(value(row.EconomieSocialeSolidaireUniteLegale)),
		MissionCompany:              parseOptionalBool(value(row.SocieteMissionUniteLegale)),
		IsEmployer:                  parseOptionalBool(value(row.CaractereEmployeurUniteLegale)),
		PrimaryActivityNAF25Code:    value(row.ActivitePrincipaleNAF25UniteLegale),
		RawPayload:                  raw,
		PayloadHash:                 hash,
	}, nil
}

func establishmentRecordFromRow(row establishmentParquetRow) (EstablishmentRecord, error) {
	siren := cleanDigits(value(row.Siren))
	nic := leftPadDigits(value(row.Nic), 5)
	siret := cleanDigits(value(row.Siret))
	if siren == "" {
		return EstablishmentRecord{}, errors.New("siren is required")
	}
	if nic == "" {
		return EstablishmentRecord{}, errors.New("nic is required")
	}
	if siret == "" {
		siret = siren + nic
	}
	raw, hash, err := canonicalPayload(establishmentRawPayload(row))
	if err != nil {
		return EstablishmentRecord{}, err
	}
	return EstablishmentRecord{
		Siren:                       siren,
		NIC:                         nic,
		Siret:                       siret,
		DiffusionStatus:             value(row.StatutDiffusionEtablissement),
		CreatedDate:                 dateValue(row.DateCreationEtablissement),
		EmployeeBand:                value(row.TrancheEffectifsEtablissement),
		EmployeeYear:                int64String(row.AnneeEffectifsEtablissement),
		CraftsRegistryActivity:      value(row.ActivitePrincipaleRegistreMetiersEtablissement),
		SourceUpdatedAt:             timeValue(row.DateDernierTraitementEtablissement),
		IsHeadquarters:              boolValue(row.EtablissementSiege),
		PeriodCount:                 int64ToInt32(row.NombrePeriodesEtablissement),
		AddressComplement:           value(row.ComplementAdresseEtablissement),
		StreetNumber:                value(row.NumeroVoieEtablissement),
		StreetNumberSuffix:          value(row.IndiceRepetitionEtablissement),
		LastStreetNumber:            value(row.DernierNumeroVoieEtablissement),
		LastStreetNumberSuffix:      value(row.IndiceRepetitionDernierNumeroVoieEtablissement),
		StreetType:                  value(row.TypeVoieEtablissement),
		StreetLabel:                 value(row.LibelleVoieEtablissement),
		PostalCode:                  value(row.CodePostalEtablissement),
		CityLabel:                   value(row.LibelleCommuneEtablissement),
		ForeignCityLabel:            value(row.LibelleCommuneEtrangerEtablissement),
		SpecialDistribution:         value(row.DistributionSpecialeEtablissement),
		CommuneCode:                 value(row.CodeCommuneEtablissement),
		CedexCode:                   value(row.CodeCedexEtablissement),
		CedexLabel:                  value(row.LibelleCedexEtablissement),
		ForeignCountryCode:          value(row.CodePaysEtrangerEtablissement),
		ForeignCountryLabel:         value(row.LibellePaysEtrangerEtablissement),
		AddressIdentifier:           value(row.IdentifiantAdresseEtablissement),
		LambertX:                    value(row.CoordonneeLambertAbscisseEtablissement),
		LambertY:                    value(row.CoordonneeLambertOrdonneeEtablissement),
		AddressComplement2:          value(row.ComplementAdresse2Etablissement),
		StreetNumber2:               value(row.NumeroVoie2Etablissement),
		StreetNumberSuffix2:         value(row.IndiceRepetition2Etablissement),
		StreetType2:                 value(row.TypeVoie2Etablissement),
		StreetLabel2:                value(row.LibelleVoie2Etablissement),
		PostalCode2:                 value(row.CodePostal2Etablissement),
		CityLabel2:                  value(row.LibelleCommune2Etablissement),
		ForeignCityLabel2:           value(row.LibelleCommuneEtranger2Etablissement),
		SpecialDistribution2:        value(row.DistributionSpeciale2Etablissement),
		CommuneCode2:                value(row.CodeCommune2Etablissement),
		CedexCode2:                  value(row.CodeCedex2Etablissement),
		CedexLabel2:                 value(row.LibelleCedex2Etablissement),
		ForeignCountryCode2:         value(row.CodePaysEtranger2Etablissement),
		ForeignCountryLabel2:        value(row.LibellePaysEtranger2Etablissement),
		PeriodStartedAt:             dateValue(row.DateDebut),
		AdministrativeStatus:        value(row.EtatAdministratifEtablissement),
		TradeName1:                  value(row.Enseigne1Etablissement),
		TradeName2:                  value(row.Enseigne2Etablissement),
		TradeName3:                  value(row.Enseigne3Etablissement),
		UsualName:                   value(row.DenominationUsuelleEtablissement),
		PrimaryActivityCode:         value(row.ActivitePrincipaleEtablissement),
		PrimaryActivityNomenclature: value(row.NomenclatureActivitePrincipaleEtablissement),
		IsEmployer:                  parseOptionalBool(value(row.CaractereEmployeurEtablissement)),
		PrimaryActivityNAF25Code:    value(row.ActivitePrincipaleNAF25Etablissement),
		RawPayload:                  raw,
		PayloadHash:                 hash,
	}, nil
}

func (r LegalUnitRecord) UpsertParams(sourceFileID pgtype.UUID, metadata []byte) db.UpsertFranceWorkflowRawLegalUnitParams {
	return db.UpsertFranceWorkflowRawLegalUnitParams{
		SourceFileID:                sourceFileID,
		SourceNativeID:              r.Siren,
		Siren:                       r.Siren,
		DiffusionStatus:             optionalString(r.DiffusionStatus),
		IsPurged:                    r.IsPurged,
		CreatedDate:                 optionalDate(r.CreatedDate),
		Acronym:                     optionalString(r.Acronym),
		Gender:                      optionalString(r.Gender),
		FirstName1:                  optionalString(r.FirstName1),
		FirstName2:                  optionalString(r.FirstName2),
		FirstName3:                  optionalString(r.FirstName3),
		FirstName4:                  optionalString(r.FirstName4),
		UsualFirstName:              optionalString(r.UsualFirstName),
		Pseudonym:                   optionalString(r.Pseudonym),
		AssociationIdentifier:       optionalString(r.AssociationIdentifier),
		EmployeeBand:                optionalString(r.EmployeeBand),
		EmployeeYear:                optionalString(r.EmployeeYear),
		SourceUpdatedAt:             optionalTimestamptz(r.SourceUpdatedAt),
		PeriodCount:                 r.PeriodCount,
		CompanyCategory:             optionalString(r.CompanyCategory),
		CompanyCategoryYear:         optionalString(r.CompanyCategoryYear),
		PeriodStartedAt:             optionalDate(r.PeriodStartedAt),
		AdministrativeStatus:        optionalString(r.AdministrativeStatus),
		BirthName:                   optionalString(r.BirthName),
		UsageName:                   optionalString(r.UsageName),
		LegalName:                   optionalString(r.LegalName),
		UsualName1:                  optionalString(r.UsualName1),
		UsualName2:                  optionalString(r.UsualName2),
		UsualName3:                  optionalString(r.UsualName3),
		LegalFormCode:               optionalString(r.LegalFormCode),
		PrimaryActivityCode:         optionalString(r.PrimaryActivityCode),
		PrimaryActivityNomenclature: optionalString(r.PrimaryActivityNomenclature),
		HeadquartersNic:             optionalString(r.HeadquartersNIC),
		SocialSolidarity:            r.SocialSolidarity,
		MissionCompany:              r.MissionCompany,
		IsEmployer:                  r.IsEmployer,
		PrimaryActivityNaf25Code:    optionalString(r.PrimaryActivityNAF25Code),
		RawPayload:                  r.RawPayload,
		PayloadHash:                 r.PayloadHash,
		Metadata:                    metadata,
	}
}

func (r EstablishmentRecord) UpsertParams(sourceFileID pgtype.UUID, metadata []byte) db.UpsertFranceWorkflowRawEstablishmentParams {
	return db.UpsertFranceWorkflowRawEstablishmentParams{
		SourceFileID:                sourceFileID,
		SourceNativeID:              r.Siret,
		Siren:                       r.Siren,
		Nic:                         r.NIC,
		Siret:                       r.Siret,
		DiffusionStatus:             optionalString(r.DiffusionStatus),
		CreatedDate:                 optionalDate(r.CreatedDate),
		EmployeeBand:                optionalString(r.EmployeeBand),
		EmployeeYear:                optionalString(r.EmployeeYear),
		CraftsRegistryActivity:      optionalString(r.CraftsRegistryActivity),
		SourceUpdatedAt:             optionalTimestamptz(r.SourceUpdatedAt),
		IsHeadquarters:              r.IsHeadquarters,
		PeriodCount:                 r.PeriodCount,
		AddressComplement:           optionalString(r.AddressComplement),
		StreetNumber:                optionalString(r.StreetNumber),
		StreetNumberSuffix:          optionalString(r.StreetNumberSuffix),
		LastStreetNumber:            optionalString(r.LastStreetNumber),
		LastStreetNumberSuffix:      optionalString(r.LastStreetNumberSuffix),
		StreetType:                  optionalString(r.StreetType),
		StreetLabel:                 optionalString(r.StreetLabel),
		PostalCode:                  optionalString(r.PostalCode),
		CityLabel:                   optionalString(r.CityLabel),
		ForeignCityLabel:            optionalString(r.ForeignCityLabel),
		SpecialDistribution:         optionalString(r.SpecialDistribution),
		CommuneCode:                 optionalString(r.CommuneCode),
		CedexCode:                   optionalString(r.CedexCode),
		CedexLabel:                  optionalString(r.CedexLabel),
		ForeignCountryCode:          optionalString(r.ForeignCountryCode),
		ForeignCountryLabel:         optionalString(r.ForeignCountryLabel),
		AddressIdentifier:           optionalString(r.AddressIdentifier),
		LambertX:                    optionalString(r.LambertX),
		LambertY:                    optionalString(r.LambertY),
		AddressComplement2:          optionalString(r.AddressComplement2),
		StreetNumber2:               optionalString(r.StreetNumber2),
		StreetNumberSuffix2:         optionalString(r.StreetNumberSuffix2),
		StreetType2:                 optionalString(r.StreetType2),
		StreetLabel2:                optionalString(r.StreetLabel2),
		PostalCode2:                 optionalString(r.PostalCode2),
		CityLabel2:                  optionalString(r.CityLabel2),
		ForeignCityLabel2:           optionalString(r.ForeignCityLabel2),
		SpecialDistribution2:        optionalString(r.SpecialDistribution2),
		CommuneCode2:                optionalString(r.CommuneCode2),
		CedexCode2:                  optionalString(r.CedexCode2),
		CedexLabel2:                 optionalString(r.CedexLabel2),
		ForeignCountryCode2:         optionalString(r.ForeignCountryCode2),
		ForeignCountryLabel2:        optionalString(r.ForeignCountryLabel2),
		PeriodStartedAt:             optionalDate(r.PeriodStartedAt),
		AdministrativeStatus:        optionalString(r.AdministrativeStatus),
		TradeName1:                  optionalString(r.TradeName1),
		TradeName2:                  optionalString(r.TradeName2),
		TradeName3:                  optionalString(r.TradeName3),
		UsualName:                   optionalString(r.UsualName),
		PrimaryActivityCode:         optionalString(r.PrimaryActivityCode),
		PrimaryActivityNomenclature: optionalString(r.PrimaryActivityNomenclature),
		IsEmployer:                  r.IsEmployer,
		PrimaryActivityNaf25Code:    optionalString(r.PrimaryActivityNAF25Code),
		RawPayload:                  r.RawPayload,
		PayloadHash:                 r.PayloadHash,
		Metadata:                    metadata,
	}
}

func legalUnitRawPayload(row legalUnitParquetRow) map[string]any {
	return map[string]any{
		"siren":                                     nullableValue(row.Siren),
		"statutDiffusionUniteLegale":                nullableValue(row.StatutDiffusionUniteLegale),
		"unitePurgeeUniteLegale":                    nullableValue(row.UnitePurgeeUniteLegale),
		"dateCreationUniteLegale":                   nullableDateValue(row.DateCreationUniteLegale),
		"sigleUniteLegale":                          nullableValue(row.SigleUniteLegale),
		"sexeUniteLegale":                           nullableValue(row.SexeUniteLegale),
		"prenom1UniteLegale":                        nullableValue(row.Prenom1UniteLegale),
		"prenom2UniteLegale":                        nullableValue(row.Prenom2UniteLegale),
		"prenom3UniteLegale":                        nullableValue(row.Prenom3UniteLegale),
		"prenom4UniteLegale":                        nullableValue(row.Prenom4UniteLegale),
		"prenomUsuelUniteLegale":                    nullableValue(row.PrenomUsuelUniteLegale),
		"pseudonymeUniteLegale":                     nullableValue(row.PseudonymeUniteLegale),
		"identifiantAssociationUniteLegale":         nullableValue(row.IdentifiantAssociationUniteLegale),
		"trancheEffectifsUniteLegale":               nullableValue(row.TrancheEffectifsUniteLegale),
		"anneeEffectifsUniteLegale":                 nullableValue(row.AnneeEffectifsUniteLegale),
		"dateDernierTraitementUniteLegale":          nullableValue(row.DateDernierTraitementUniteLegale),
		"nombrePeriodesUniteLegale":                 nullableValue(row.NombrePeriodesUniteLegale),
		"categorieEntreprise":                       nullableValue(row.CategorieEntreprise),
		"anneeCategorieEntreprise":                  nullableValue(row.AnneeCategorieEntreprise),
		"dateDebut":                                 nullableDateValue(row.DateDebut),
		"etatAdministratifUniteLegale":              nullableValue(row.EtatAdministratifUniteLegale),
		"nomUniteLegale":                            nullableValue(row.NomUniteLegale),
		"nomUsageUniteLegale":                       nullableValue(row.NomUsageUniteLegale),
		"denominationUniteLegale":                   nullableValue(row.DenominationUniteLegale),
		"denominationUsuelle1UniteLegale":           nullableValue(row.DenominationUsuelle1UniteLegale),
		"denominationUsuelle2UniteLegale":           nullableValue(row.DenominationUsuelle2UniteLegale),
		"denominationUsuelle3UniteLegale":           nullableValue(row.DenominationUsuelle3UniteLegale),
		"categorieJuridiqueUniteLegale":             nullableValue(row.CategorieJuridiqueUniteLegale),
		"activitePrincipaleUniteLegale":             nullableValue(row.ActivitePrincipaleUniteLegale),
		"nomenclatureActivitePrincipaleUniteLegale": nullableValue(row.NomenclatureActivitePrincipaleUniteLegale),
		"nicSiegeUniteLegale":                       nullableValue(row.NicSiegeUniteLegale),
		"economieSocialeSolidaireUniteLegale":       nullableValue(row.EconomieSocialeSolidaireUniteLegale),
		"societeMissionUniteLegale":                 nullableValue(row.SocieteMissionUniteLegale),
		"caractereEmployeurUniteLegale":             nullableValue(row.CaractereEmployeurUniteLegale),
		"activitePrincipaleNAF25UniteLegale":        nullableValue(row.ActivitePrincipaleNAF25UniteLegale),
	}
}

func establishmentRawPayload(row establishmentParquetRow) map[string]any {
	return map[string]any{
		"siren":                         nullableValue(row.Siren),
		"nic":                           nullableValue(row.Nic),
		"siret":                         nullableValue(row.Siret),
		"statutDiffusionEtablissement":  nullableValue(row.StatutDiffusionEtablissement),
		"dateCreationEtablissement":     nullableDateValue(row.DateCreationEtablissement),
		"trancheEffectifsEtablissement": nullableValue(row.TrancheEffectifsEtablissement),
		"anneeEffectifsEtablissement":   nullableValue(row.AnneeEffectifsEtablissement),
		"activitePrincipaleRegistreMetiersEtablissement": nullableValue(row.ActivitePrincipaleRegistreMetiersEtablissement),
		"dateDernierTraitementEtablissement":             nullableValue(row.DateDernierTraitementEtablissement),
		"etablissementSiege":                             nullableValue(row.EtablissementSiege),
		"nombrePeriodesEtablissement":                    nullableValue(row.NombrePeriodesEtablissement),
		"complementAdresseEtablissement":                 nullableValue(row.ComplementAdresseEtablissement),
		"numeroVoieEtablissement":                        nullableValue(row.NumeroVoieEtablissement),
		"indiceRepetitionEtablissement":                  nullableValue(row.IndiceRepetitionEtablissement),
		"dernierNumeroVoieEtablissement":                 nullableValue(row.DernierNumeroVoieEtablissement),
		"indiceRepetitionDernierNumeroVoieEtablissement": nullableValue(row.IndiceRepetitionDernierNumeroVoieEtablissement),
		"typeVoieEtablissement":                          nullableValue(row.TypeVoieEtablissement),
		"libelleVoieEtablissement":                       nullableValue(row.LibelleVoieEtablissement),
		"codePostalEtablissement":                        nullableValue(row.CodePostalEtablissement),
		"libelleCommuneEtablissement":                    nullableValue(row.LibelleCommuneEtablissement),
		"libelleCommuneEtrangerEtablissement":            nullableValue(row.LibelleCommuneEtrangerEtablissement),
		"distributionSpecialeEtablissement":              nullableValue(row.DistributionSpecialeEtablissement),
		"codeCommuneEtablissement":                       nullableValue(row.CodeCommuneEtablissement),
		"codeCedexEtablissement":                         nullableValue(row.CodeCedexEtablissement),
		"libelleCedexEtablissement":                      nullableValue(row.LibelleCedexEtablissement),
		"codePaysEtrangerEtablissement":                  nullableValue(row.CodePaysEtrangerEtablissement),
		"libellePaysEtrangerEtablissement":               nullableValue(row.LibellePaysEtrangerEtablissement),
		"identifiantAdresseEtablissement":                nullableValue(row.IdentifiantAdresseEtablissement),
		"coordonneeLambertAbscisseEtablissement":         nullableValue(row.CoordonneeLambertAbscisseEtablissement),
		"coordonneeLambertOrdonneeEtablissement":         nullableValue(row.CoordonneeLambertOrdonneeEtablissement),
		"complementAdresse2Etablissement":                nullableValue(row.ComplementAdresse2Etablissement),
		"numeroVoie2Etablissement":                       nullableValue(row.NumeroVoie2Etablissement),
		"indiceRepetition2Etablissement":                 nullableValue(row.IndiceRepetition2Etablissement),
		"typeVoie2Etablissement":                         nullableValue(row.TypeVoie2Etablissement),
		"libelleVoie2Etablissement":                      nullableValue(row.LibelleVoie2Etablissement),
		"codePostal2Etablissement":                       nullableValue(row.CodePostal2Etablissement),
		"libelleCommune2Etablissement":                   nullableValue(row.LibelleCommune2Etablissement),
		"libelleCommuneEtranger2Etablissement":           nullableValue(row.LibelleCommuneEtranger2Etablissement),
		"distributionSpeciale2Etablissement":             nullableValue(row.DistributionSpeciale2Etablissement),
		"codeCommune2Etablissement":                      nullableValue(row.CodeCommune2Etablissement),
		"codeCedex2Etablissement":                        nullableValue(row.CodeCedex2Etablissement),
		"libelleCedex2Etablissement":                     nullableValue(row.LibelleCedex2Etablissement),
		"codePaysEtranger2Etablissement":                 nullableValue(row.CodePaysEtranger2Etablissement),
		"libellePaysEtranger2Etablissement":              nullableValue(row.LibellePaysEtranger2Etablissement),
		"dateDebut":                                      nullableDateValue(row.DateDebut),
		"etatAdministratifEtablissement":                 nullableValue(row.EtatAdministratifEtablissement),
		"enseigne1Etablissement":                         nullableValue(row.Enseigne1Etablissement),
		"enseigne2Etablissement":                         nullableValue(row.Enseigne2Etablissement),
		"enseigne3Etablissement":                         nullableValue(row.Enseigne3Etablissement),
		"denominationUsuelleEtablissement":               nullableValue(row.DenominationUsuelleEtablissement),
		"activitePrincipaleEtablissement":                nullableValue(row.ActivitePrincipaleEtablissement),
		"nomenclatureActivitePrincipaleEtablissement":    nullableValue(row.NomenclatureActivitePrincipaleEtablissement),
		"caractereEmployeurEtablissement":                nullableValue(row.CaractereEmployeurEtablissement),
		"activitePrincipaleNAF25Etablissement":           nullableValue(row.ActivitePrincipaleNAF25Etablissement),
	}
}

func canonicalPayload(payload map[string]any) (json.RawMessage, string, error) {
	canonical, err := json.Marshal(payload)
	if err != nil {
		return nil, "", errors.Wrap(err, "encode france sirene raw payload")
	}
	hash := sha256.Sum256(canonical)
	return canonical, hex.EncodeToString(hash[:]), nil
}

func nullableValue[T any](input *T) any {
	if input == nil {
		return nil
	}
	switch value := any(*input).(type) {
	case string:
		return strings.TrimSpace(value)
	case time.Time:
		return value.Format(time.RFC3339Nano)
	default:
		return value
	}
}

func value(input *string) string {
	if input == nil {
		return ""
	}
	return strings.TrimSpace(*input)
}

func cleanDigits(input string) string {
	var builder strings.Builder
	for _, r := range input {
		if r >= '0' && r <= '9' {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func leftPadDigits(input string, width int) string {
	digits := cleanDigits(input)
	if digits == "" || len(digits) >= width {
		return digits
	}
	return strings.Repeat("0", width-len(digits)) + digits
}

func boolValue(input *bool) *bool {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}

func int64String(input *int64) string {
	if input == nil {
		return ""
	}
	return strconv.FormatInt(*input, 10)
}

func int64ToInt32(input *int64) *int32 {
	if input == nil || *input < 0 || *input > int64(^uint32(0)>>1) {
		return nil
	}
	value := int32(*input)
	return &value
}

func dateValue(input *int32) *time.Time {
	if input == nil {
		return nil
	}
	value := parquetDateEpoch.AddDate(0, 0, int(*input))
	return &value
}

func nullableDateValue(input *int32) any {
	value := dateValue(input)
	if value == nil {
		return nil
	}
	return value.Format(time.DateOnly)
}

func timeValue(input *time.Time) *time.Time {
	if input == nil || input.IsZero() {
		return nil
	}
	value := *input
	return &value
}

func parseOptionalBool(input string) *bool {
	normalized := strings.ToLower(strings.TrimSpace(input))
	if normalized == "" {
		return nil
	}
	switch normalized {
	case "true", "t", "1", "o", "oui", "y", "yes":
		value := true
		return &value
	case "false", "f", "0", "n", "non", "no":
		value := false
		return &value
	default:
		return nil
	}
}

func optionalString(input string) *string {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func optionalDate(input *time.Time) pgtype.Date {
	if input == nil {
		return pgtype.Date{}
	}
	return pgtype.Date{Time: *input, Valid: true}
}

func optionalTimestamptz(input *time.Time) pgtype.Timestamptz {
	if input == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *input, Valid: true}
}
