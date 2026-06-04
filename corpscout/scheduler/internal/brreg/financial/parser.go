package financial

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"
)

const annualAccountsStatementType = "annual_accounts"

func ParseKeyFigures(raw []byte, orgNumber string, baseURL string) ([]Statement, error) {
	var rawRecords []json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&rawRecords); err != nil {
		return nil, fmt.Errorf("decode brreg key figures: %w", err)
	}
	statements := make([]Statement, 0, len(rawRecords))
	for _, rawRecord := range rawRecords {
		record, err := decodeRecord(rawRecord)
		if err != nil {
			return nil, fmt.Errorf("decode brreg record: %w", err)
		}
		statements = append(statements, statementFromRecord(record, rawRecord, orgNumber, baseURL))
	}
	return statements, nil
}

func ParseUnsupportedPlan(body []byte) (string, bool) {
	var response struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return "", false
	}
	const marker = "Regnskapet inneholder en oppstillingsplan som ikke er stottet ("
	index := strings.Index(response.Message, marker)
	if index < 0 {
		return "", false
	}
	rest := response.Message[index+len(marker):]
	end := strings.Index(rest, ")")
	if end < 0 {
		return "", false
	}
	return rest[:end], true
}

type brregRecord struct {
	ID            json.Number `json:"id"`
	JournalNr     string      `json:"journalnr"`
	Regnskapstype string      `json:"regnskapstype"`
	Virksomhet    struct {
		Organisasjonsnummer string `json:"organisasjonsnummer"`
		Organisasjonsform   string `json:"organisasjonsform"`
		Morselskap          bool   `json:"morselskap"`
	} `json:"virksomhet"`
	Regnskapsperiode struct {
		FraDato string `json:"fraDato"`
		TilDato string `json:"tilDato"`
	} `json:"regnskapsperiode"`
	Valuta             string `json:"valuta"`
	Avviklingsregnskap bool   `json:"avviklingsregnskap"`
	Oppstillingsplan   string `json:"oppstillingsplan"`
	Revisjon           struct {
		IkkeRevidertAarsregnskap bool `json:"ikkeRevidertAarsregnskap"`
		FravalgRevisjon          bool `json:"fravalgRevisjon"`
	} `json:"revisjon"`
	Regnkapsprinsipper struct {
		SmaaForetak     bool   `json:"smaaForetak"`
		Regnskapsregler string `json:"regnskapsregler"`
	} `json:"regnkapsprinsipper"`
	Resultatregnskap *brregResultat     `json:"resultatregnskapResultat"`
	Eiendeler        *brregEiendeler    `json:"eiendeler"`
	EgKapGjeld       *brregEgenkapGjeld `json:"egenkapitalGjeld"`
}

type brregResultat struct {
	OrdinaertFoerSkatt json.Number  `json:"ordinaertResultatFoerSkattekostnad"`
	Skattekostnad      json.Number  `json:"ordinaertResultatSkattekostnad"`
	Aarsresultat       json.Number  `json:"aarsresultat"`
	Totalresultat      json.Number  `json:"totalresultat"`
	Finansresultat     *brregFinans `json:"finansresultat"`
	Driftsresultat     *brregDrift  `json:"driftsresultat"`
}

type brregFinans struct {
	NettoFinans   json.Number `json:"nettoFinans"`
	Finansinntekt *struct {
		Sum json.Number `json:"sumFinansinntekter"`
	} `json:"finansinntekt"`
	Finanskostnad *struct {
		Sum json.Number `json:"sumFinanskostnad"`
	} `json:"finanskostnad"`
}

type brregDrift struct {
	Driftsresultat  json.Number   `json:"driftsresultat"`
	Driftsinntekter *brregInntekt `json:"driftsinntekter"`
}

type brregInntekt struct {
	Sum            json.Number `json:"sumDriftsinntekter"`
	Salgsinntekter json.Number `json:"salgsinntekter"`
}

type brregEiendeler struct {
	Sum    json.Number `json:"sumEiendeler"`
	Omloep *struct {
		Sum json.Number `json:"sumOmloepsmidler"`
	} `json:"omloepsmidler"`
	Anlegg *struct {
		Sum json.Number `json:"sumAnleggsmidler"`
	} `json:"anleggsmidler"`
}

type brregEgenkapGjeld struct {
	Egenkapital *struct {
		Sum json.Number `json:"sumEgenkapital"`
	} `json:"egenkapital"`
	GjeldOversikt *struct {
		Sum        json.Number `json:"sumGjeld"`
		Kortsiktig *struct {
			Sum json.Number `json:"sumKortsiktigGjeld"`
		} `json:"kortsiktigGjeld"`
		Langsiktig *struct {
			Sum json.Number `json:"sumLangsiktigGjeld"`
		} `json:"langsiktigGjeld"`
	} `json:"gjeldOversikt"`
}

func decodeRecord(raw json.RawMessage) (brregRecord, error) {
	var record brregRecord
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return record, decoder.Decode(&record)
}

func statementFromRecord(record brregRecord, raw json.RawMessage, orgNumber string, baseURL string) Statement {
	hash := sha256.Sum256(raw)
	sourceRecordID := record.ID.String()
	statement := Statement{
		SourceRecordID:   sourceRecordID,
		JournalNumber:    record.JournalNr,
		FiscalYear:       fiscalYear(record.Regnskapsperiode.TilDato),
		PeriodStart:      record.Regnskapsperiode.FraDato,
		PeriodEnd:        record.Regnskapsperiode.TilDato,
		StatementType:    annualAccountsStatementType,
		IsConsolidated:   record.Regnskapstype == "KONSERN",
		OriginalCurrency: strings.ToUpper(strings.TrimSpace(record.Valuta)),
		Metadata: StatementMetadata{
			OrganizationForm:    record.Virksomhet.Organisasjonsform,
			IsParentCompany:     record.Virksomhet.Morselskap,
			SourceStatementType: record.Regnskapstype,
			StatementPlan:       record.Oppstillingsplan,
			AccountingRules:     record.Regnkapsprinsipper.Regnskapsregler,
			SmallCompany:        record.Regnkapsprinsipper.SmaaForetak,
			NotAudited:          record.Revisjon.IkkeRevidertAarsregnskap,
			AuditOptOut:         record.Revisjon.FravalgRevisjon,
			LiquidationAccounts: record.Avviklingsregnskap,
			JournalNumber:       record.JournalNr,
			SourceRecordID:      sourceRecordID,
		},
		Evidence: StatementEvidence{
			Source:         "brreg_regnskapsregisteret",
			SourceURL:      fmt.Sprintf("%s/regnskapsregisteret/regnskap/%s", baseURL, orgNumber),
			DetailURL:      fmt.Sprintf("%s/regnskapsregisteret/regnskap/%s/%s", baseURL, orgNumber, sourceRecordID),
			RawPayloadHash: "sha256:" + hex.EncodeToString(hash[:]),
		},
		RawPayload: json.RawMessage(raw),
	}
	if result := record.Resultatregnskap; result != nil {
		if operating := result.Driftsresultat; operating != nil {
			if operating.Driftsinntekter != nil {
				statement.RevenueOriginalAmount = numberToDecimalString(operating.Driftsinntekter.Sum)
				statement.OperatingIncomeOriginalAmount = numberToDecimalString(operating.Driftsinntekter.Salgsinntekter)
			}
			statement.OperatingProfitOriginalAmount = numberToDecimalString(operating.Driftsresultat)
		}
		statement.ProfitBeforeTaxOriginalAmount = numberToDecimalString(result.OrdinaertFoerSkatt)
		statement.TaxExpenseOriginalAmount = numberToDecimalString(result.Skattekostnad)
		statement.NetIncomeOriginalAmount = numberToDecimalString(result.Aarsresultat)
		statement.TotalResultOriginalAmount = numberToDecimalString(result.Totalresultat)
		addFinancialFacts(&statement, result.Finansresultat)
	}
	if assets := record.Eiendeler; assets != nil {
		statement.TotalAssetsOriginalAmount = numberToDecimalString(assets.Sum)
		if assets.Omloep != nil {
			statement.CurrentAssetsOriginalAmount = numberToDecimalString(assets.Omloep.Sum)
		}
		if assets.Anlegg != nil {
			statement.FixedAssetsOriginalAmount = numberToDecimalString(assets.Anlegg.Sum)
		}
	}
	if equityLiabilities := record.EgKapGjeld; equityLiabilities != nil {
		if equityLiabilities.Egenkapital != nil {
			statement.TotalEquityOriginalAmount = numberToDecimalString(equityLiabilities.Egenkapital.Sum)
		}
		if liabilities := equityLiabilities.GjeldOversikt; liabilities != nil {
			statement.TotalLiabilitiesOriginalAmount = numberToDecimalString(liabilities.Sum)
			if liabilities.Kortsiktig != nil {
				statement.CurrentLiabilitiesOriginalAmount = numberToDecimalString(liabilities.Kortsiktig.Sum)
			}
			if liabilities.Langsiktig != nil {
				statement.LongTermLiabilitiesOriginalAmount = numberToDecimalString(liabilities.Langsiktig.Sum)
			}
		}
	}
	return statement
}

func addFinancialFacts(statement *Statement, financialResult *brregFinans) {
	if financialResult == nil {
		return
	}
	facts := make(map[string]string)
	if value := numberToDecimalString(financialResult.NettoFinans); value != nil {
		facts["finance_result_original_amount"] = *value
	}
	if financialResult.Finansinntekt != nil {
		if value := numberToDecimalString(financialResult.Finansinntekt.Sum); value != nil {
			facts["financial_income_original_amount"] = *value
		}
	}
	if financialResult.Finanskostnad != nil {
		if value := numberToDecimalString(financialResult.Finanskostnad.Sum); value != nil {
			facts["financial_cost_original_amount"] = *value
		}
	}
	if len(facts) > 0 {
		statement.Facts = facts
	}
}

func numberToDecimalString(number json.Number) *string {
	if number == "" {
		return nil
	}
	rat, ok := new(big.Rat).SetString(number.String())
	if !ok {
		return nil
	}
	value := rat.FloatString(2)
	return &value
}

func fiscalYear(periodEnd string) int32 {
	parsed, err := time.Parse("2006-01-02", periodEnd)
	if err != nil {
		return 0
	}
	return int32(parsed.Year())
}
