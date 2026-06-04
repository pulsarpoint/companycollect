package financial

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseKeyFiguresNormalizesAnnualAccountsForSourceTable(t *testing.T) {
	statements, err := ParseKeyFigures([]byte(`[
  {
    "id": 5667197,
    "journalnr": "2024-123",
    "regnskapstype": "KONSERN",
    "virksomhet": {
      "organisasjonsnummer": "923609016",
      "organisasjonsform": "ASA",
      "morselskap": true
    },
    "regnskapsperiode": {
      "fraDato": "2024-01-01",
      "tilDato": "2024-12-31"
    },
    "valuta": "NOK",
    "avviklingsregnskap": false,
    "oppstillingsplan": "STORE",
    "revisjon": {
      "ikkeRevidertAarsregnskap": false,
      "fravalgRevisjon": false
    },
    "regnkapsprinsipper": {
      "smaaForetak": false,
      "regnskapsregler": "IFRS"
    },
    "resultatregnskapResultat": {
      "ordinaertResultatFoerSkattekostnad": 300.5,
      "ordinaertResultatSkattekostnad": 75,
      "aarsresultat": 225.5,
      "totalresultat": 220,
      "finansresultat": {
        "nettoFinans": 50,
        "finansinntekt": {"sumFinansinntekter": 60},
        "finanskostnad": {"sumFinanskostnad": 10}
      },
      "driftsresultat": {
        "driftsresultat": 250,
        "driftsinntekter": {
          "sumDriftsinntekter": 1000,
          "salgsinntekter": 950
        }
      }
    },
    "eiendeler": {
      "sumEiendeler": 5000,
      "omloepsmidler": {"sumOmloepsmidler": 2000},
      "anleggsmidler": {"sumAnleggsmidler": 3000}
    },
    "egenkapitalGjeld": {
      "egenkapital": {"sumEgenkapital": 1500},
      "gjeldOversikt": {
        "sumGjeld": 3500,
        "kortsiktigGjeld": {"sumKortsiktigGjeld": 1200},
        "langsiktigGjeld": {"sumLangsiktigGjeld": 2300}
      }
    }
  }
]`), "923609016", "https://data.brreg.no")

	require.NoError(t, err)
	require.Len(t, statements, 1)
	statement := statements[0]
	require.Equal(t, "5667197", statement.SourceRecordID)
	require.Equal(t, "2024-123", statement.JournalNumber)
	require.Equal(t, int32(2024), statement.FiscalYear)
	require.Equal(t, "2024-01-01", statement.PeriodStart)
	require.Equal(t, "2024-12-31", statement.PeriodEnd)
	require.Equal(t, "annual_accounts", statement.StatementType)
	require.True(t, statement.IsConsolidated)
	require.Equal(t, "NOK", statement.OriginalCurrency)
	require.Equal(t, "1000.00", requireString(t, statement.RevenueOriginalAmount))
	require.Equal(t, "950.00", requireString(t, statement.OperatingIncomeOriginalAmount))
	require.Equal(t, "250.00", requireString(t, statement.OperatingProfitOriginalAmount))
	require.Equal(t, "300.50", requireString(t, statement.ProfitBeforeTaxOriginalAmount))
	require.Equal(t, "225.50", requireString(t, statement.NetIncomeOriginalAmount))
	require.Equal(t, "5000.00", requireString(t, statement.TotalAssetsOriginalAmount))
	require.Equal(t, "1200.00", requireString(t, statement.CurrentLiabilitiesOriginalAmount))
	require.Equal(t, "2300.00", requireString(t, statement.LongTermLiabilitiesOriginalAmount))
	require.Equal(t, "50.00", statement.Facts["finance_result_original_amount"])
	require.Equal(t, "brreg_regnskapsregisteret", statement.Evidence.Source)
	require.Equal(t, "https://data.brreg.no/regnskapsregisteret/regnskap/923609016", statement.Evidence.SourceURL)
	require.Equal(t, "https://data.brreg.no/regnskapsregisteret/regnskap/923609016/5667197", statement.Evidence.DetailURL)
	require.NotEmpty(t, statement.Evidence.RawPayloadHash)
	require.NotEmpty(t, statement.RawPayload)
}

func TestParseUnsupportedPlan(t *testing.T) {
	plan, ok := ParseUnsupportedPlan([]byte(`{"message":"Regnskapet inneholder en oppstillingsplan som ikke er stottet (BANK)"}`))

	require.True(t, ok)
	require.Equal(t, "BANK", plan)
}

func requireString(t *testing.T, value *string) string {
	t.Helper()
	require.NotNil(t, value)
	return *value
}
