package service_test

import (
	"context"
	"os"
	"testing"

	"github.com/pulsarpoint/brreg-financial-service/internal/brregclient"
	"github.com/pulsarpoint/brreg-financial-service/internal/models"
	"github.com/pulsarpoint/brreg-financial-service/internal/service"
	"github.com/stretchr/testify/require"
)

type stubClient struct {
	keyFigures map[string][]byte
	keyErrors  map[string]error
	pdfYears   map[string][]string
	pdfErrors  map[string]error
}

func (s *stubClient) FetchKeyFigures(_ context.Context, orgNum string) ([]byte, error) {
	if err, ok := s.keyErrors[orgNum]; ok {
		return nil, err
	}
	return s.keyFigures[orgNum], nil
}

func (s *stubClient) FetchPDFYears(_ context.Context, orgNum string) ([]string, error) {
	if err, ok := s.pdfErrors[orgNum]; ok {
		return nil, err
	}
	if yrs, ok := s.pdfYears[orgNum]; ok {
		return yrs, nil
	}
	return nil, brregclient.ErrNotAvailable
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../parser/testdata/" + name)
	require.NoError(t, err)
	return b
}

func TestLookup_SingleSuccess(t *testing.T) {
	c := &stubClient{
		keyFigures: map[string][]byte{"923609016": fixture(t, "equinor_list.json")},
		pdfYears:   map[string][]string{"923609016": {"2022", "2023", "2024"}},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records:            []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "923609016"}},
		IncludePDFMetadata: true,
		IncludeRawPayload:  false,
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, "brreg-financial-service.lookup.v1", resp.SchemaVersion)
	require.Equal(t, "succeeded", resp.Status)
	require.Equal(t, 1, resp.RecordsSeen)
	require.Equal(t, 1, resp.RecordsCompleted)
	require.Equal(t, 0, resp.RecordsFailed)
	require.Len(t, resp.Results, 1)

	r := resp.Results[0]
	require.Equal(t, "r1", r.RecordID)
	require.Equal(t, "succeeded", r.Status)
	require.Len(t, r.Statements, 1)
	require.NotNil(t, r.PDFMetadata)
	require.Equal(t, []string{"2022", "2023", "2024"}, r.PDFMetadata.AvailableYears)
	require.Contains(t, r.PDFMetadata.DownloadURLTemplate, "923609016")
	require.Empty(t, r.Warnings)

	// raw_payload not included when IncludeRawPayload=false
	require.Nil(t, r.Statements[0].RawPayload)
}

func TestLookup_IncludeRawPayload(t *testing.T) {
	c := &stubClient{
		keyFigures: map[string][]byte{"923609016": fixture(t, "equinor_list.json")},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records:           []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "923609016"}},
		IncludeRawPayload: true,
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp.Results[0].Statements[0].RawPayload)
}

func TestLookup_NotAvailable(t *testing.T) {
	c := &stubClient{
		keyErrors: map[string]error{"974760673": brregclient.ErrNotAvailable},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records: []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "974760673"}},
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, "succeeded", resp.Status) // not_available is a clean outcome
	require.Equal(t, 1, resp.RecordsCompleted)
	require.Equal(t, 0, resp.RecordsFailed)
	r := resp.Results[0]
	require.Equal(t, "not_available", r.Status)
	require.Empty(t, r.Statements)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, "financials_not_found", r.Warnings[0].Code)
}

func TestLookup_UnsupportedPlan_WithPDFYears(t *testing.T) {
	c := &stubClient{
		keyErrors: map[string]error{"984851006": &brregclient.UnsupportedPlanError{PlanName: "BANK"}},
		pdfYears:  map[string][]string{"984851006": {"2023", "2024"}},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records:            []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "984851006"}},
		IncludePDFMetadata: true,
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, "succeeded", resp.Status)
	r := resp.Results[0]
	require.Equal(t, "unsupported_statement_plan", r.Status)
	require.Empty(t, r.Statements)
	require.NotNil(t, r.PDFMetadata) // PDF still returned
	require.Equal(t, []string{"2023", "2024"}, r.PDFMetadata.AvailableYears)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, "unsupported_statement_plan", r.Warnings[0].Code)
	require.Equal(t, "BANK", r.Warnings[0].Detail["statement_plan"])
}

func TestLookup_ProviderFailure(t *testing.T) {
	c := &stubClient{
		keyErrors: map[string]error{"000000001": &brregclient.RetryableError{StatusCode: 503, Msg: "down"}},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records: []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "000000001"}},
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, "failed", resp.Status)
	require.Equal(t, 0, resp.RecordsCompleted)
	require.Equal(t, 1, resp.RecordsFailed)
	r := resp.Results[0]
	require.Equal(t, "failed", r.Status)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, "provider_unavailable", r.Warnings[0].Code)
}

func TestLookup_MixedBatch_PartialStatus(t *testing.T) {
	c := &stubClient{
		keyFigures: map[string][]byte{"923609016": fixture(t, "equinor_list.json")},
		keyErrors:  map[string]error{"000000001": &brregclient.RetryableError{StatusCode: 503, Msg: "down"}},
	}
	svc := service.New(c, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})

	req := models.LookupRequest{
		Records: []models.LookupRecord{
			{RecordID: "ok", OrganizationNumber: "923609016"},
			{RecordID: "fail", OrganizationNumber: "000000001"},
		},
	}
	resp, err := svc.Lookup(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "partial", resp.Status)
	require.Equal(t, 1, resp.RecordsCompleted)
	require.Equal(t, 1, resp.RecordsFailed)
}

func TestLookup_BatchTooLarge(t *testing.T) {
	svc := service.New(&stubClient{}, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 2})
	records := make([]models.LookupRecord, 3)
	for i := range records {
		records[i] = models.LookupRecord{RecordID: "x", OrganizationNumber: "923609016"}
	}
	_, err := svc.Lookup(context.Background(), models.LookupRequest{Records: records})
	require.Error(t, err)
	require.Contains(t, err.Error(), "batch_too_large")
}

func TestLookup_InvalidOrgNumber(t *testing.T) {
	svc := service.New(&stubClient{}, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 1000})
	_, err := svc.Lookup(context.Background(), models.LookupRequest{
		Records: []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "not-a-number"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid_organization_number")
}
