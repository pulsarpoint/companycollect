package brreg

import (
	"context"
	"errors"
	"testing"
)

func TestInsertTextTranslationsUsesChunkedClickHouseBatches(t *testing.T) {
	rows := []TextTranslation{
		fixtureTextTranslation(1),
		fixtureTextTranslation(2),
		fixtureTextTranslation(3),
		fixtureTextTranslation(4),
		fixtureTextTranslation(5),
	}
	factory := &fakeTextTranslationBatchFactory{}

	inserted, err := insertTextTranslationsBatched(context.Background(), rows, 2, factory.prepare)
	if err != nil {
		t.Fatalf("insert text translations: %v", err)
	}

	if inserted != len(rows) {
		t.Fatalf("expected %d inserted rows, got %d", len(rows), inserted)
	}
	if len(factory.batches) != 3 {
		t.Fatalf("expected 3 ClickHouse batches, got %d", len(factory.batches))
	}
	assertFakeBatch(t, factory.batches[0], 2)
	assertFakeBatch(t, factory.batches[1], 2)
	assertFakeBatch(t, factory.batches[2], 1)
}

func TestInsertTextTranslationsAbortsCurrentBatchOnAppendError(t *testing.T) {
	rows := []TextTranslation{
		fixtureTextTranslation(1),
		fixtureTextTranslation(2),
	}
	factory := &fakeTextTranslationBatchFactory{appendErrAfter: 1}

	inserted, err := insertTextTranslationsBatched(context.Background(), rows, 10, factory.prepare)
	if err == nil {
		t.Fatal("expected append error")
	}
	if inserted != 0 {
		t.Fatalf("expected 0 inserted rows before append error, got %d", inserted)
	}
	if len(factory.batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(factory.batches))
	}
	if !factory.batches[0].aborted {
		t.Fatal("expected failed batch to be aborted")
	}
	if factory.batches[0].sent {
		t.Fatal("failed batch must not be sent")
	}
}

func fixtureTextTranslation(hash uint64) TextTranslation {
	return TextTranslation{
		SourceTable:    SourceTable,
		SourceColumn:   ActivityTextColumn,
		SourceText:     "source",
		SourceTextHash: hash,
		SourceLang:     SourceLang,
		TargetLang:     TargetLang,
		TranslatedText: "translated",
		Provider:       "local",
		Model:          "qwen3:6b",
		Version:        123,
	}
}

func assertFakeBatch(t *testing.T, batch *fakeTextTranslationBatch, wantRows int) {
	t.Helper()

	if len(batch.rows) != wantRows {
		t.Fatalf("expected %d appended rows, got %d", wantRows, len(batch.rows))
	}
	if !batch.sent {
		t.Fatal("expected batch to be sent")
	}
	if batch.aborted {
		t.Fatal("sent batch must not be aborted")
	}
}

type fakeTextTranslationBatchFactory struct {
	batches        []*fakeTextTranslationBatch
	appendErrAfter int
}

func (f *fakeTextTranslationBatchFactory) prepare(ctx context.Context) (textTranslationBatch, error) {
	batch := &fakeTextTranslationBatch{appendErrAfter: f.appendErrAfter}
	f.batches = append(f.batches, batch)
	return batch, nil
}

type fakeTextTranslationBatch struct {
	rows           [][]any
	appendErrAfter int
	sent           bool
	aborted        bool
}

func (b *fakeTextTranslationBatch) Append(values ...any) error {
	if b.appendErrAfter > 0 && len(b.rows) >= b.appendErrAfter {
		return errors.New("append failed")
	}
	b.rows = append(b.rows, values)
	return nil
}

func (b *fakeTextTranslationBatch) Send() error {
	b.sent = true
	return nil
}

func (b *fakeTextTranslationBatch) Abort() error {
	b.aborted = true
	return nil
}
