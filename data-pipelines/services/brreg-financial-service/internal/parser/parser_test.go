package parser_test

import (
	"os"
	"testing"

	"github.com/pulsarpoint/brreg-financial-service/internal/parser"
	"github.com/stretchr/testify/require"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return b
}

// --- ParseKeyFigures ---

func TestParseKeyFigures_Equinor(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "equinor_list.json"), "923609016", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)

	s := records[0]
	require.Equal(t, "5667197", s.SourceRecordID)
	require.Equal(t, "2025428073", s.JournalNumber)
	require.Equal(t, 2024, s.FiscalYear)
	require.Equal(t, "2024-01-01", s.PeriodStart)
	require.Equal(t, "2024-12-31", s.PeriodEnd)
	require.Equal(t, "company", s.StatementType)
	require.Equal(t, "USD", s.OriginalCurrency)

	// amounts preserved as decimal strings, 2dp
	require.NotNil(t, s.RevenueOriginalAmount)
	require.Equal(t, "72543000000.00", *s.RevenueOriginalAmount)
	require.Nil(t, s.SalesRevenueOriginalAmount)   // not present in source
	require.Nil(t, s.TotalResultOriginalAmount)    // not present in source

	require.NotNil(t, s.OperatingProfitOriginalAmount)
	require.Equal(t, "10347000000.00", *s.OperatingProfitOriginalAmount)

	require.NotNil(t, s.TotalAssetsOriginalAmount)
	require.Equal(t, "109150000000.00", *s.TotalAssetsOriginalAmount)

	require.NotNil(t, s.TotalEquityOriginalAmount)
	require.Equal(t, "41090000000.00", *s.TotalEquityOriginalAmount)

	// facts
	require.Contains(t, s.Facts, "finance_result_original_amount")
	require.Equal(t, "-2179000000.00", s.Facts["finance_result_original_amount"])
	require.Equal(t, "516000000.00", s.Facts["financial_income_original_amount"])
	require.Equal(t, "2695000000.00", s.Facts["financial_cost_original_amount"])

	// metadata
	require.Equal(t, "ASA", s.Metadata.OrganizationForm)
	require.True(t, s.Metadata.IsParentCompany)
	require.Equal(t, "store", s.Metadata.StatementPlan)
	require.Equal(t, "forenkletAnvendelseIFRS", s.Metadata.AccountingRules)
	require.False(t, s.Metadata.SmallCompany)
	require.False(t, s.Metadata.LiquidationAccounts)

	// evidence
	require.Equal(t, "brreg_regnskapsregisteret", s.Evidence.Source)
	require.Equal(t, "https://data.brreg.no/regnskapsregisteret/regnskap/923609016", s.Evidence.SourceURL)
	require.Equal(t, "https://data.brreg.no/regnskapsregisteret/regnskap/923609016/5667197", s.Evidence.DetailURL)
	require.NotEmpty(t, s.Evidence.RawPayloadHash)
	require.Contains(t, s.Evidence.RawPayloadHash, "sha256:")
}

func TestParseKeyFigures_AkerBP_HasTotalresultat(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "akerbp_list.json"), "989795848", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	s := records[0]
	require.NotNil(t, s.TotalResultOriginalAmount)
	require.Equal(t, "1818000000.00", *s.TotalResultOriginalAmount)
	require.Equal(t, "USD", s.OriginalCurrency)
	require.Equal(t, "IFRS", s.Metadata.AccountingRules)
}

func TestParseKeyFigures_BaneNor_NegativeOperatingProfit(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "banenor_list.json"), "917082308", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	s := records[0]
	require.Equal(t, "NOK", s.OriginalCurrency)
	require.Equal(t, "SF", s.Metadata.OrganizationForm)
	require.NotNil(t, s.OperatingProfitOriginalAmount)
	require.Equal(t, "-15000000.00", *s.OperatingProfitOriginalAmount)
}

func TestParseKeyFigures_Bortigard_SmallCompany(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "bortigard_list.json"), "810202572", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	s := records[0]
	require.True(t, s.Metadata.SmallCompany)
	// small amounts should still round-trip correctly
	require.Equal(t, "174012.00", *s.RevenueOriginalAmount)
	require.Equal(t, "6059747.00", *s.TotalAssetsOriginalAmount)
}

func TestParseKeyFigures_Nel_NegativeNetIncome(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "nel_list.json"), "915501680", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	s := records[0]
	require.NotNil(t, s.NetIncomeOriginalAmount)
	require.Equal(t, "-260874000.00", *s.NetIncomeOriginalAmount)
	require.NotNil(t, s.ProfitBeforeTaxOriginalAmount)
	require.Equal(t, "-260742000.00", *s.ProfitBeforeTaxOriginalAmount)
}

func TestParseKeyFigures_Mowi_EUR(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "mowi_list.json"), "964118191", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "EUR", records[0].OriginalCurrency)
}

func TestParseKeyFigures_Konsern_StatementTypeGroup(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "konsern_list.json"), "923609016", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "group", records[0].StatementType)
}

func TestParseKeyFigures_NoRevenue_AllNull(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "no_revenue_list.json"), "999999999", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	s := records[0]
	require.Nil(t, s.RevenueOriginalAmount)
	require.Nil(t, s.OperatingProfitOriginalAmount)
	require.Nil(t, s.TotalAssetsOriginalAmount)
	require.Empty(t, s.Facts)
}

func TestParseKeyFigures_AuditOptOut(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "audit_optout_list.json"), "888888888", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.True(t, records[0].Metadata.AuditOptOut)
	require.False(t, records[0].Metadata.NotAudited)
}

func TestParseKeyFigures_Liquidation(t *testing.T) {
	records, err := parser.ParseKeyFigures(fixture(t, "liquidation_list.json"), "777777777", "https://data.brreg.no")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.True(t, records[0].Metadata.LiquidationAccounts)
}

func TestParseKeyFigures_EmptyArray(t *testing.T) {
	records, err := parser.ParseKeyFigures([]byte("[]"), "000000000", "https://data.brreg.no")
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestParseKeyFigures_RawPayloadHash_Deterministic(t *testing.T) {
	data := fixture(t, "equinor_list.json")
	r1, _ := parser.ParseKeyFigures(data, "923609016", "https://data.brreg.no")
	r2, _ := parser.ParseKeyFigures(data, "923609016", "https://data.brreg.no")
	require.Equal(t, r1[0].Evidence.RawPayloadHash, r2[0].Evidence.RawPayloadHash)
}

// --- ParseUnsupportedPlan ---

func TestParseUnsupportedPlan_BANK(t *testing.T) {
	plan, ok := parser.ParseUnsupportedPlan(fixture(t, "dnb_500.json"))
	require.True(t, ok)
	require.Equal(t, "BANK", plan)
}

func TestParseUnsupportedPlan_SKADE(t *testing.T) {
	plan, ok := parser.ParseUnsupportedPlan(fixture(t, "storebrand_500.json"))
	require.True(t, ok)
	require.Equal(t, "SKADE", plan)
}

func TestParseUnsupportedPlan_GenericError(t *testing.T) {
	body := []byte(`{"status":"500","message":"internal server error"}`)
	_, ok := parser.ParseUnsupportedPlan(body)
	require.False(t, ok)
}

// --- ParsePDFYears ---

func TestParsePDFYears_Equinor(t *testing.T) {
	years, err := parser.ParsePDFYears(fixture(t, "equinor_pdf_years.json"))
	require.NoError(t, err)
	require.Len(t, years, 14)
	require.Equal(t, "2011", years[0])
	require.Equal(t, "2024", years[13])
}

func TestParsePDFYears_DNB(t *testing.T) {
	years, err := parser.ParsePDFYears(fixture(t, "dnb_pdf_years.json"))
	require.NoError(t, err)
	require.Len(t, years, 15)
}

func TestParsePDFYears_Empty(t *testing.T) {
	years, err := parser.ParsePDFYears([]byte("[]"))
	require.NoError(t, err)
	require.Empty(t, years)
}
