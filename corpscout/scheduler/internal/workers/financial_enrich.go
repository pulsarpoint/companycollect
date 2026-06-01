package workers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/riverqueue/river"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/fxrates"
)

const (
	financialEnrichmentSourceBrreg      = "brreg"
	financialEnrichmentCurrencyNOK      = "NOK"
	financialEnrichmentInputTable       = "brreg_financial_accounts"
	financialEnrichmentEvidenceKind     = "financial"
	financialEnrichmentOperationAdd     = "add"
	financialEnrichmentHTTPHeaderAccept = "Accept"
	financialEnrichmentAcceptJSON       = "application/json"
)

type FinancialEnrichWorker struct {
	river.WorkerDefaults[EnrichCompanyFinancialsArgs]
	db            db.Querier
	fetchAccounts brregAccountsFetcher
	loadRates     fxRateLoader
}

type brregAccountsFetcher func(context.Context, string) ([]brregAccount, error)

type fxRateLoader func(context.Context) (usdConverter, error)

type usdConverter interface {
	ToUSD(amount int64, currency string) (int64, error)
}

type financialEnrichmentEvidence struct {
	Source         string `json:"source"`
	SourceNativeID string `json:"source_native_id"`
	Kind           string `json:"kind"`
}

func NewFinancialEnrichWorker(q db.Querier) *FinancialEnrichWorker {
	return newFinancialEnrichWorker(
		q,
		fetchBrregAccounts,
		func(ctx context.Context) (usdConverter, error) {
			return fxrates.Load(ctx)
		},
	)
}

func newFinancialEnrichWorker(q db.Querier, fetcher brregAccountsFetcher, loader fxRateLoader) *FinancialEnrichWorker {
	if fetcher == nil {
		fetcher = fetchBrregAccounts
	}
	if loader == nil {
		loader = func(ctx context.Context) (usdConverter, error) {
			return fxrates.Load(ctx)
		}
	}
	return &FinancialEnrichWorker{db: q, fetchAccounts: fetcher, loadRates: loader}
}

func (w *FinancialEnrichWorker) Work(ctx context.Context, job *river.Job[EnrichCompanyFinancialsArgs]) error {
	args := job.Args
	companyID, err := uuid.Parse(args.CompanyID)
	if err != nil {
		return errors.Wrap(err, "parse company id")
	}

	if args.SourceName != financialEnrichmentSourceBrreg {
		return errors.Newf("financial enrichment source %q is not implemented", args.SourceName)
	}

	accounts, err := w.fetchAccounts(ctx, args.OrgNumber)
	if err != nil {
		return errors.Wrap(err, "fetch brreg financial accounts")
	}
	if len(accounts) == 0 {
		slog.Info("no regnskap accounts found", "org", args.OrgNumber)
		return nil
	}
	src, err := w.db.GetSourceByName(ctx, args.SourceName)
	if err != nil {
		return errors.Wrap(err, "get financial enrichment source")
	}

	acc := accounts[0] // most recent

	revenueOrig := int64(acc.Revenue * 100)
	profitOrig := int64(acc.Profit * 100)

	var revenueUSDPtr, profitUSDPtr *int64
	rates, err := w.loadRates(ctx)
	if err != nil {
		slog.Warn("fxrates load failed - storing without USD conversion", "error", err)
	} else {
		if rev, err := rates.ToUSD(revenueOrig, financialEnrichmentCurrencyNOK); err == nil {
			revenueUSDPtr = &rev
		}
		if prf, err := rates.ToUSD(profitOrig, financialEnrichmentCurrencyNOK); err == nil {
			profitUSDPtr = &prf
		}
	}

	year := int32(acc.Year)
	currency := financialEnrichmentCurrencyNOK
	evidence, err := financialEnrichmentEvidencePayload(args.SourceName, args.OrgNumber)
	if err != nil {
		return errors.Wrap(err, "build financial evidence")
	}
	payloadHash := financialSuggestionHash(args.OrgNumber, year, revenueOrig, profitOrig)
	suggestion, err := w.db.InsertSuggestion(ctx, db.InsertSuggestionParams{
		TargetCompanyID:   pgUUID(companyID),
		SourceID:          src.ID,
		SourceType:        src.Name,
		SourceInputTable:  financialEnrichmentInputTable,
		SourceInputID:     fmt.Sprintf("%s:%d", args.OrgNumber, year),
		SourceNativeID:    nullableText(args.OrgNumber),
		SourcePayloadHash: &payloadHash,
		Confidence:        ptrFloat32(0.85),
	})
	if err != nil {
		return errors.Wrap(err, "create financial suggestion parent")
	}
	_, err = w.db.InsertSuggestionCompanyFinancial(ctx, db.InsertSuggestionCompanyFinancialParams{
		SuggestionID:    suggestion.ID,
		Operation:       financialEnrichmentOperationAdd,
		Confidence:      ptrFloat32(0.85),
		Year:            year,
		SourceName:      args.SourceName,
		RevenueAmount:   &revenueOrig,
		RevenueCurrency: &currency,
		RevenueUsd:      revenueUSDPtr,
		ProfitAmount:    &profitOrig,
		ProfitUsd:       profitUSDPtr,
		Evidence:        evidence,
	})
	if err != nil {
		return errors.Wrap(err, "create company financial suggestion")
	}
	slog.Info("company financial suggestion created",
		"company_id", args.CompanyID,
		"org_number", args.OrgNumber,
		"year", year,
		"revenue_orig_cents", revenueOrig,
	)
	return nil
}

func financialEnrichmentEvidencePayload(sourceName, orgNumber string) ([]byte, error) {
	return json.Marshal(financialEnrichmentEvidence{
		Source:         sourceName,
		SourceNativeID: orgNumber,
		Kind:           financialEnrichmentEvidenceKind,
	})
}

func financialSuggestionHash(orgNumber string, year int32, revenueOrig, profitOrig int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d:%d", orgNumber, year, revenueOrig, profitOrig)))
	return hex.EncodeToString(sum[:])
}

type brregAccount struct {
	Year    int
	Revenue float64
	Profit  float64
}

type brregRegnskapDTO struct {
	Regnskapsperiode struct {
		TilDato string `json:"tilDato"`
	} `json:"regnskapsperiode"`
	ResultatregnskapResultat struct {
		Driftsresultat struct {
			Driftsinntekter struct {
				SumDriftsinntekter *float64 `json:"sumDriftsinntekter"`
			} `json:"driftsinntekter"`
		} `json:"driftsresultat"`
		OrdinaertResultatFoerSkattekostnad *float64 `json:"ordinaertResultatFoerSkattekostnad"`
	} `json:"resultatregnskapResultat"`
}

func fetchBrregAccounts(ctx context.Context, orgNumber string) ([]brregAccount, error) {
	url := fmt.Sprintf("https://data.brreg.no/regnskapsregisteret/regnskap/%s", orgNumber)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "build brreg financial request")
	}
	req.Header.Set(financialEnrichmentHTTPHeaderAccept, financialEnrichmentAcceptJSON)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "fetch brreg financial accounts")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, errors.Newf("brreg returned %d: %s", resp.StatusCode, string(b))
	}

	var dtos []brregRegnskapDTO
	if err := json.NewDecoder(resp.Body).Decode(&dtos); err != nil {
		return nil, errors.Wrap(err, "decode brreg response")
	}

	accounts := make([]brregAccount, 0, len(dtos))
	for _, d := range dtos {
		year := 0
		if len(d.Regnskapsperiode.TilDato) >= 4 {
			fmt.Sscanf(d.Regnskapsperiode.TilDato[:4], "%d", &year)
		}
		var revenue, profit float64
		if v := d.ResultatregnskapResultat.Driftsresultat.Driftsinntekter.SumDriftsinntekter; v != nil {
			revenue = *v
		}
		if v := d.ResultatregnskapResultat.OrdinaertResultatFoerSkattekostnad; v != nil {
			profit = *v
		}
		accounts = append(accounts, brregAccount{Year: year, Revenue: revenue, Profit: profit})
	}
	return accounts, nil
}
