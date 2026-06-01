package workers

import (
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type financialQuerier struct {
	db.Querier
	getSourceByName                  func(context.Context, string) (db.DataSource, error)
	insertSuggestion                 func(context.Context, db.InsertSuggestionParams) (db.Suggestion, error)
	insertSuggestionCompanyFinancial func(context.Context, db.InsertSuggestionCompanyFinancialParams) (db.SuggestionCompanyFinancial, error)
}

func (q *financialQuerier) GetSourceByName(ctx context.Context, name string) (db.DataSource, error) {
	return q.getSourceByName(ctx, name)
}

func (q *financialQuerier) InsertSuggestion(ctx context.Context, arg db.InsertSuggestionParams) (db.Suggestion, error) {
	return q.insertSuggestion(ctx, arg)
}

func (q *financialQuerier) InsertSuggestionCompanyFinancial(ctx context.Context, arg db.InsertSuggestionCompanyFinancialParams) (db.SuggestionCompanyFinancial, error) {
	return q.insertSuggestionCompanyFinancial(ctx, arg)
}

type fixedUSDRates struct{}

func (fixedUSDRates) ToUSD(amount int64, currency string) (int64, error) {
	if currency != "NOK" {
		return 0, errors.Newf("unexpected currency %q", currency)
	}
	return amount / 10, nil
}

func TestFinancialEnrichWorkerUsesNamedConstants(t *testing.T) {
	source, err := os.ReadFile("financial_enrich.go")
	require.NoError(t, err)
	body := string(source)

	require.NotContains(t, body, `args.SourceName != "brreg"`)
	require.NotContains(t, body, `rates.ToUSD(revenueOrig, "NOK")`)
	require.NotContains(t, body, `rates.ToUSD(profitOrig, "NOK")`)
	require.NotContains(t, body, `currency := "NOK"`)
	require.NotContains(t, body, `SourceInputTable:  "brreg_financial_accounts"`)
	require.NotContains(t, body, `Header.Set("Accept", "application/json")`)
	require.NotContains(t, body, `"source":           args.SourceName`)
	require.NotContains(t, body, `"source_native_id": args.OrgNumber`)
	require.NotContains(t, body, `"kind":             financialEnrichmentEvidenceKind`)
	require.Contains(t, body, "financialEnrichmentSourceBrreg")
	require.Contains(t, body, "financialEnrichmentCurrencyNOK")
	require.Contains(t, body, "financialEnrichmentInputTable")
	require.Contains(t, body, "financialEnrichmentHTTPHeaderAccept")
	require.Contains(t, body, "financialEnrichmentAcceptJSON")
	require.Contains(t, body, "type financialEnrichmentEvidence struct")
	require.Contains(t, body, "financialEnrichmentEvidencePayload")
}

func TestFinancialEnrichWorkerUsesInjectedDependencies(t *testing.T) {
	ctx := context.Background()
	companyID := uuid.New()
	sourceID := uuid.New()
	suggestionID := uuid.New()

	var insertedSuggestion db.InsertSuggestionParams
	var insertedFinancial db.InsertSuggestionCompanyFinancialParams
	q := &financialQuerier{
		getSourceByName: func(_ context.Context, name string) (db.DataSource, error) {
			require.Equal(t, "brreg", name)
			return db.DataSource{ID: sourceID, Name: name}, nil
		},
		insertSuggestion: func(_ context.Context, arg db.InsertSuggestionParams) (db.Suggestion, error) {
			insertedSuggestion = arg
			return db.Suggestion{ID: suggestionID}, nil
		},
		insertSuggestionCompanyFinancial: func(_ context.Context, arg db.InsertSuggestionCompanyFinancialParams) (db.SuggestionCompanyFinancial, error) {
			insertedFinancial = arg
			return db.SuggestionCompanyFinancial{}, nil
		},
	}

	worker := newFinancialEnrichWorker(
		q,
		func(_ context.Context, orgNumber string) ([]brregAccount, error) {
			require.Equal(t, "810202572", orgNumber)
			return []brregAccount{{Year: 2024, Revenue: 1200.50, Profit: 100.25}}, nil
		},
		func(context.Context) (usdConverter, error) {
			return fixedUSDRates{}, nil
		},
	)

	err := worker.Work(ctx, &river.Job[EnrichCompanyFinancialsArgs]{
		Args: EnrichCompanyFinancialsArgs{
			CompanyID:  companyID.String(),
			OrgNumber:  "810202572",
			SourceName: "brreg",
		},
	})

	require.NoError(t, err)
	require.True(t, insertedSuggestion.TargetCompanyID.Valid)
	require.Equal(t, [16]byte(companyID), insertedSuggestion.TargetCompanyID.Bytes)
	require.Equal(t, sourceID, insertedSuggestion.SourceID)
	require.Equal(t, "brreg_financial_accounts", insertedSuggestion.SourceInputTable)
	require.Equal(t, "810202572:2024", insertedSuggestion.SourceInputID)
	require.Equal(t, suggestionID, insertedFinancial.SuggestionID)
	require.Equal(t, int32(2024), insertedFinancial.Year)
	require.Equal(t, int64(120050), *insertedFinancial.RevenueAmount)
	require.Equal(t, int64(12005), *insertedFinancial.RevenueUsd)
	require.Equal(t, int64(10025), *insertedFinancial.ProfitAmount)
	require.Equal(t, int64(1002), *insertedFinancial.ProfitUsd)
	require.JSONEq(t, `{"source":"brreg","source_native_id":"810202572","kind":"financial"}`, string(insertedFinancial.Evidence))
}

func TestFinancialEnrichWorkerSkipsWhenNoAccountsAreFound(t *testing.T) {
	calledDB := false
	worker := newFinancialEnrichWorker(
		&financialQuerier{
			getSourceByName: func(context.Context, string) (db.DataSource, error) {
				calledDB = true
				return db.DataSource{}, nil
			},
		},
		func(context.Context, string) ([]brregAccount, error) {
			return nil, nil
		},
		nil,
	)

	err := worker.Work(context.Background(), &river.Job[EnrichCompanyFinancialsArgs]{
		Args: EnrichCompanyFinancialsArgs{
			CompanyID:  uuid.NewString(),
			OrgNumber:  "810202572",
			SourceName: "brreg",
		},
	})

	require.NoError(t, err)
	require.False(t, calledDB)
}

func TestFinancialEnrichWorkerRejectsUnsupportedSourceBeforeFetching(t *testing.T) {
	fetched := false
	worker := newFinancialEnrichWorker(
		&financialQuerier{},
		func(context.Context, string) ([]brregAccount, error) {
			fetched = true
			return nil, nil
		},
		nil,
	)

	err := worker.Work(context.Background(), &river.Job[EnrichCompanyFinancialsArgs]{
		Args: EnrichCompanyFinancialsArgs{
			CompanyID:  uuid.NewString(),
			OrgNumber:  "123",
			SourceName: "companies_house",
		},
	})

	require.ErrorContains(t, err, `financial enrichment source "companies_house" is not implemented`)
	require.False(t, fetched)
}
