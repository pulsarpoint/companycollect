package parser

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pulsarpoint/brreg-financial-service/internal/models"
	"github.com/shopspring/decimal"
)

// ParseKeyFigures parses the raw BRREG list-endpoint response body.
// orgNum and baseURL are used to build evidence URLs.
// Returns an empty slice (no error) for a valid empty array.
func ParseKeyFigures(raw []byte, orgNum, baseURL string) ([]models.Statement, error) {
	var rawRecords []json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&rawRecords); err != nil {
		return nil, fmt.Errorf("decode brreg key figures: %w", err)
	}
	out := make([]models.Statement, 0, len(rawRecords))
	for _, r := range rawRecords {
		rec, err := decodeRecord(r)
		if err != nil {
			return nil, fmt.Errorf("decode brreg record: %w", err)
		}
		out = append(out, toStatement(rec, r, orgNum, baseURL))
	}
	return out, nil
}

// ParseUnsupportedPlan inspects a BRREG HTTP 500 response body.
// Returns (planName, true) when the error indicates an unsupported statement plan,
// e.g. "Regnskapet inneholder en oppstillingsplan som ikke er stottet (BANK)".
func ParseUnsupportedPlan(body []byte) (string, bool) {
	var resp struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", false
	}
	const marker = "Regnskapet inneholder en oppstillingsplan som ikke er stottet ("
	idx := strings.Index(resp.Message, marker)
	if idx < 0 {
		return "", false
	}
	rest := resp.Message[idx+len(marker):]
	end := strings.Index(rest, ")")
	if end < 0 {
		return "", false
	}
	return rest[:end], true
}

// ParsePDFYears parses the BRREG PDF-years list endpoint response body.
// Returns a slice of year strings, e.g. ["2011","2012",...].
func ParsePDFYears(raw []byte) ([]string, error) {
	var years []string
	if err := json.Unmarshal(raw, &years); err != nil {
		return nil, fmt.Errorf("decode pdf years: %w", err)
	}
	return years, nil
}

// --- internal types matching the BRREG key-figure JSON schema ---

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
	// NOTE: "regnkapsprinsipper" is the actual BRREG API field name (missing 's' is their typo)
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
	var rec brregRecord
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return rec, dec.Decode(&rec)
}

func toStatement(rec brregRecord, raw json.RawMessage, orgNum, baseURL string) models.Statement {
	hash := sha256.Sum256(raw)
	hashStr := "sha256:" + hex.EncodeToString(hash[:])

	fiscalYear := extractFiscalYear(rec.Regnskapsperiode.TilDato)

	s := models.Statement{
		SourceRecordID:   rec.ID.String(),
		JournalNumber:    rec.JournalNr,
		FiscalYear:       fiscalYear,
		PeriodStart:      rec.Regnskapsperiode.FraDato,
		PeriodEnd:        rec.Regnskapsperiode.TilDato,
		StatementType:    mapStatementType(rec.Regnskapstype),
		OriginalCurrency: rec.Valuta,
		Metadata: models.StatementMetadata{
			OrganizationForm:    rec.Virksomhet.Organisasjonsform,
			IsParentCompany:     rec.Virksomhet.Morselskap,
			StatementPlan:       rec.Oppstillingsplan,
			AccountingRules:     rec.Regnkapsprinsipper.Regnskapsregler,
			SmallCompany:        rec.Regnkapsprinsipper.SmaaForetak,
			NotAudited:          rec.Revisjon.IkkeRevidertAarsregnskap,
			AuditOptOut:         rec.Revisjon.FravalgRevisjon,
			LiquidationAccounts: rec.Avviklingsregnskap,
		},
		Evidence: models.StatementEvidence{
			Source:         "brreg_regnskapsregisteret",
			SourceURL:      fmt.Sprintf("%s/regnskapsregisteret/regnskap/%s", baseURL, orgNum),
			DetailURL:      fmt.Sprintf("%s/regnskapsregisteret/regnskap/%s/%s", baseURL, orgNum, rec.ID.String()),
			RawPayloadHash: hashStr,
		},
		RawPayload: json.RawMessage(raw),
	}

	if r := rec.Resultatregnskap; r != nil {
		if d := r.Driftsresultat; d != nil {
			if d.Driftsinntekter != nil {
				s.RevenueOriginalAmount = numToStr(d.Driftsinntekter.Sum)
				s.SalesRevenueOriginalAmount = numToStr(d.Driftsinntekter.Salgsinntekter)
			}
			s.OperatingProfitOriginalAmount = numToStr(d.Driftsresultat)
		}
		s.ProfitBeforeTaxOriginalAmount = numToStr(r.OrdinaertFoerSkatt)
		s.TaxExpenseOriginalAmount = numToStr(r.Skattekostnad)
		s.NetIncomeOriginalAmount = numToStr(r.Aarsresultat)
		s.TotalResultOriginalAmount = numToStr(r.Totalresultat)

		facts := make(map[string]string)
		if f := r.Finansresultat; f != nil {
			if v := numToStr(f.NettoFinans); v != nil {
				facts["finance_result_original_amount"] = *v
			}
			if f.Finansinntekt != nil {
				if v := numToStr(f.Finansinntekt.Sum); v != nil {
					facts["financial_income_original_amount"] = *v
				}
			}
			if f.Finanskostnad != nil {
				if v := numToStr(f.Finanskostnad.Sum); v != nil {
					facts["financial_cost_original_amount"] = *v
				}
			}
		}
		if len(facts) > 0 {
			s.Facts = facts
		}
	}

	if e := rec.Eiendeler; e != nil {
		s.TotalAssetsOriginalAmount = numToStr(e.Sum)
		if e.Omloep != nil {
			s.CurrentAssetsOriginalAmount = numToStr(e.Omloep.Sum)
		}
		if e.Anlegg != nil {
			s.FixedAssetsOriginalAmount = numToStr(e.Anlegg.Sum)
		}
	}

	if eg := rec.EgKapGjeld; eg != nil {
		if eg.Egenkapital != nil {
			s.TotalEquityOriginalAmount = numToStr(eg.Egenkapital.Sum)
		}
		if g := eg.GjeldOversikt; g != nil {
			s.TotalLiabilitiesOriginalAmount = numToStr(g.Sum)
			if g.Kortsiktig != nil {
				s.ShortTermLiabilitiesOriginalAmount = numToStr(g.Kortsiktig.Sum)
			}
			if g.Langsiktig != nil {
				s.LongTermLiabilitiesOriginalAmount = numToStr(g.Langsiktig.Sum)
			}
		}
	}

	return s
}

// numToStr converts a json.Number to a *string decimal (2dp). Returns nil for empty/zero-value numbers.
func numToStr(n json.Number) *string {
	if n == "" {
		return nil
	}
	d, err := decimal.NewFromString(n.String())
	if err != nil {
		return nil
	}
	s := d.StringFixed(2)
	return &s
}

func mapStatementType(t string) string {
	switch t {
	case "SELSKAP":
		return "company"
	case "KONSERN":
		return "group"
	default:
		return t
	}
}

func extractFiscalYear(tilDato string) int {
	t, err := time.Parse("2006-01-02", tilDato)
	if err != nil {
		return 0
	}
	return t.Year()
}
