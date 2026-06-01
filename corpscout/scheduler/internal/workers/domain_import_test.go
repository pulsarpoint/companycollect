package workers_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"testing"

	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

// fakeS3 is an in-memory replacement for s3client.Client used in worker tests.
type fakeS3 struct {
	objects map[string][]byte
}

func newFakeS3() *fakeS3 { return &fakeS3{objects: make(map[string][]byte)} }

func (f *fakeS3) Upload(ctx context.Context, key string, body []byte, _ string) error {
	f.objects[key] = body
	return nil
}

func (f *fakeS3) Download(ctx context.Context, key string) ([]byte, string, error) {
	data, ok := f.objects[key]
	if !ok {
		return nil, "", pgx.ErrNoRows
	}
	return data, "text/csv", nil
}

func buildCSV(rows [][]string) []byte {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.WriteAll(rows)
	w.Flush()
	return buf.Bytes()
}

func TestDomainImportWorkerUsesNamedConstants(t *testing.T) {
	source, err := os.ReadFile("domain_import.go")
	require.NoError(t, err)
	body := string(source)

	require.NotContains(t, body, `const evidenceJSON`)
	require.NotContains(t, body, `finalStatus := "completed"`)
	require.NotContains(t, body, `finalStatus = "failed"`)
	require.NotContains(t, body, `ImportSource: "manual_upload"`)
	require.NotContains(t, body, `RelationshipType: "candidate"`)
	require.NotContains(t, body, `Status:           "needs_review"`)
	require.NotContains(t, body, `Signal:           "manual_upload"`)
	require.NotContains(t, body, `len(rec) < 2`)
	require.NotContains(t, body, `rec[1]`)
	require.NotContains(t, body, `rec[2]`)
	require.NotContains(t, body, "domainImportEvidenceJSON")
	require.NotContains(t, body, "`{\"source\":\"`")
	require.Contains(t, body, "domainImportBatchStatusCompleted")
	require.Contains(t, body, "domainImportSourceManualUpload")
	require.Contains(t, body, "domainImportRelationshipCandidate")
	require.Contains(t, body, "type domainImportEvidence struct")
	require.Contains(t, body, "domainImportEvidencePayload")
	require.Contains(t, body, "parseDomainImportCSVRow")
}

// importQuerier is a configurable db.Querier for domain import tests.
type importQuerier struct {
	db.Querier
	updateBatchStarted    func(ctx context.Context, arg db.UpdateImportBatchStartedParams) error
	updateBatchCompleted  func(ctx context.Context, arg db.UpdateImportBatchCompletedParams) error
	upsertDomainWithSrc   func(ctx context.Context, arg db.UpsertDomainWithSourceParams) (db.Domain, error)
	getCompanyByExactName func(ctx context.Context, name string) (db.Company, error)
	upsertCompanyDomain   func(ctx context.Context, arg db.UpsertCompanyDomainParams) (db.CompanyDomain, error)
}

func (q *importQuerier) UpdateImportBatchStarted(ctx context.Context, arg db.UpdateImportBatchStartedParams) error {
	if q.updateBatchStarted != nil {
		return q.updateBatchStarted(ctx, arg)
	}
	return nil
}

func (q *importQuerier) UpdateImportBatchCompleted(ctx context.Context, arg db.UpdateImportBatchCompletedParams) error {
	if q.updateBatchCompleted != nil {
		return q.updateBatchCompleted(ctx, arg)
	}
	return nil
}

func (q *importQuerier) UpsertDomainWithSource(ctx context.Context, arg db.UpsertDomainWithSourceParams) (db.Domain, error) {
	if q.upsertDomainWithSrc != nil {
		return q.upsertDomainWithSrc(ctx, arg)
	}
	return db.Domain{ID: uuid.New(), Domain: arg.Domain}, nil
}

func (q *importQuerier) GetCompanyByExactName(ctx context.Context, name string) (db.Company, error) {
	if q.getCompanyByExactName != nil {
		return q.getCompanyByExactName(ctx, name)
	}
	return db.Company{}, pgx.ErrNoRows
}

func (q *importQuerier) UpsertCompanyDomain(ctx context.Context, arg db.UpsertCompanyDomainParams) (db.CompanyDomain, error) {
	if q.upsertCompanyDomain != nil {
		return q.upsertCompanyDomain(ctx, arg)
	}
	return db.CompanyDomain{}, nil
}

func TestDomainImportWorker_NoCompany_JustInsertsDomain(t *testing.T) {
	ctx := context.Background()
	batchID := uuid.New()
	s3 := newFakeS3()
	csvKey := "imports/test.csv"
	s3.objects[csvKey] = buildCSV([][]string{
		{"num", "domain", "company"},
		{"1", "example.com", ""},
	})

	domainUpserted := false
	q := &importQuerier{
		upsertDomainWithSrc: func(_ context.Context, arg db.UpsertDomainWithSourceParams) (db.Domain, error) {
			assert.Equal(t, "example.com", arg.Domain)
			assert.Equal(t, "manual_upload", arg.ImportSource)
			domainUpserted = true
			return db.Domain{ID: uuid.New(), Domain: arg.Domain}, nil
		},
	}

	w := workers.NewDomainImportWorker(q, s3)
	job := &river.Job[workers.DomainImportArgs]{
		Args: workers.DomainImportArgs{BatchID: batchID.String(), CsvS3Key: csvKey},
	}
	err := w.Work(ctx, job)
	require.NoError(t, err)
	assert.True(t, domainUpserted, "domain should be upserted with import_source=manual_upload")
}

func TestDomainImportWorker_WithKnownCompany_LinksCompanyDomain(t *testing.T) {
	ctx := context.Background()
	batchID := uuid.New()
	companyID := uuid.New()
	s3 := newFakeS3()
	csvKey := "imports/test2.csv"
	s3.objects[csvKey] = buildCSV([][]string{
		{"num", "domain", "company"},
		{"1", "acme.com", "Acme Corp"},
	})

	linkedCompanyID := uuid.UUID{}
	q := &importQuerier{
		getCompanyByExactName: func(_ context.Context, name string) (db.Company, error) {
			assert.Equal(t, "Acme Corp", name)
			return db.Company{ID: companyID, Name: name}, nil
		},
		upsertCompanyDomain: func(_ context.Context, arg db.UpsertCompanyDomainParams) (db.CompanyDomain, error) {
			linkedCompanyID = arg.CompanyID
			assert.Equal(t, "manual_upload", arg.Signal)
			assert.Equal(t, int16(90), arg.Confidence)
			assert.Equal(t, "needs_review", arg.Status)
			assert.JSONEq(t, `{"source":"csv_upload"}`, string(arg.Evidence))
			return db.CompanyDomain{}, nil
		},
	}

	w := workers.NewDomainImportWorker(q, s3)
	job := &river.Job[workers.DomainImportArgs]{
		Args: workers.DomainImportArgs{BatchID: batchID.String(), CsvS3Key: csvKey},
	}
	err := w.Work(ctx, job)
	require.NoError(t, err)
	assert.Equal(t, companyID, linkedCompanyID, "company domain link should use the found company's ID")
}

func TestDomainImportWorker_WithUnknownCompany_SkipsLinking(t *testing.T) {
	ctx := context.Background()
	batchID := uuid.New()
	s3 := newFakeS3()
	csvKey := "imports/test3.csv"
	s3.objects[csvKey] = buildCSV([][]string{
		{"num", "domain", "company"},
		{"1", "newco.com", "Unknown Corp"},
	})

	linkAttempted := false
	q := &importQuerier{
		// getCompanyByExactName returns ErrNoRows (default) — company not found
		upsertCompanyDomain: func(_ context.Context, _ db.UpsertCompanyDomainParams) (db.CompanyDomain, error) {
			linkAttempted = true
			return db.CompanyDomain{}, nil
		},
	}

	w := workers.NewDomainImportWorker(q, s3)
	job := &river.Job[workers.DomainImportArgs]{
		Args: workers.DomainImportArgs{BatchID: batchID.String(), CsvS3Key: csvKey},
	}
	err := w.Work(ctx, job)
	require.NoError(t, err)
	assert.False(t, linkAttempted, "company domain link should NOT be created when company is not found")
}
