// Package engine implements the shared translation queue: loaders enqueue
// batches of untranslated text over HTTP, the Temporal workflow processes
// pending batches through a translation provider, and periodic flushes write
// finished translations into ClickHouse before deleting the matching rows
// from the local DuckDB queue.
package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// MaxEnqueueItems caps one enqueue request; loaders chunk larger loads.
const MaxEnqueueItems = 10000

type EnqueueItem struct {
	SourceTable    string `json:"source_table"`
	SourceColumn   string `json:"source_column"`
	SourceText     string `json:"source_text"`
	SourceTextHash string `json:"source_text_hash"` // decimal uint64
}

type EnqueueRequest struct {
	SourceLang         string        `json:"source_lang"`
	TargetLang         string        `json:"target_lang"`
	SourceLanguageName string        `json:"source_language_name"`
	TargetLanguageName string        `json:"target_language_name"`
	Items              []EnqueueItem `json:"items"`
}

type EnqueueResult struct {
	Received int `json:"received"`
	Inserted int `json:"inserted"`
}

type QueueStats struct {
	Input   int `json:"input"`
	Pending int `json:"pending"`
	Output  int `json:"output"`
	Failed  int `json:"failed"`
}

// Validate checks the whole request before any write; the request is
// all-or-nothing.
func (r EnqueueRequest) Validate() error {
	switch {
	case strings.TrimSpace(r.SourceLang) == "":
		return errors.New("source_lang is required")
	case strings.TrimSpace(r.TargetLang) == "":
		return errors.New("target_lang is required")
	case strings.TrimSpace(r.SourceLanguageName) == "":
		return errors.New("source_language_name is required")
	case strings.TrimSpace(r.TargetLanguageName) == "":
		return errors.New("target_language_name is required")
	case len(r.Items) == 0:
		return errors.New("at least one item is required")
	case len(r.Items) > MaxEnqueueItems:
		return fmt.Errorf("at most %d items per request, got %d", MaxEnqueueItems, len(r.Items))
	}

	for i, item := range r.Items {
		if strings.TrimSpace(item.SourceTable) == "" {
			return fmt.Errorf("items[%d]: source_table is required", i)
		}
		if strings.TrimSpace(item.SourceColumn) == "" {
			return fmt.Errorf("items[%d]: source_column is required", i)
		}
		if item.SourceText == "" {
			return fmt.Errorf("items[%d]: source_text is required", i)
		}
		if _, err := strconv.ParseUint(item.SourceTextHash, 10, 64); err != nil {
			return fmt.Errorf("items[%d]: source_text_hash must be a decimal uint64: %v", i, err)
		}
	}
	return nil
}

// Enqueue validates and upserts one loader batch into the queue. Inserted
// reports rows actually added (duplicates by dedup key are skipped).
func (r *Runtime) Enqueue(ctx context.Context, req EnqueueRequest) (EnqueueResult, error) {
	if r.closed {
		return EnqueueResult{}, errors.New("translator runtime is closed")
	}
	if err := req.Validate(); err != nil {
		return EnqueueResult{}, err
	}

	// Trim once for the whole request — these four fields are request-level,
	// not per-item, so there is no need to re-trim inside the loop below.
	sourceLang := strings.TrimSpace(req.SourceLang)
	targetLang := strings.TrimSpace(req.TargetLang)
	sourceLanguageName := strings.TrimSpace(req.SourceLanguageName)
	targetLanguageName := strings.TrimSpace(req.TargetLanguageName)

	rows := make([]InputItem, 0, len(req.Items))
	for _, item := range req.Items {
		// Validate already confirmed this parses; this call cannot fail in
		// practice, but the error is still handled defensively.
		hash, err := strconv.ParseUint(item.SourceTextHash, 10, 64)
		if err != nil {
			return EnqueueResult{}, fmt.Errorf("parse source_text_hash %q: %w", item.SourceTextHash, err)
		}
		rows = append(rows, InputItem{
			SourceTable:        item.SourceTable,
			SourceColumn:       item.SourceColumn,
			SourceText:         item.SourceText,
			SourceTextHash:     hash,
			SourceLang:         sourceLang,
			TargetLang:         targetLang,
			SourceLanguageName: sourceLanguageName,
			TargetLanguageName: targetLanguageName,
		})
	}

	before, err := countRows(ctx, r.db, "input_items")
	if err != nil {
		return EnqueueResult{}, err
	}
	if err := upsertInputItems(ctx, r.db, rows); err != nil {
		return EnqueueResult{}, err
	}
	after, err := countRows(ctx, r.db, "input_items")
	if err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{Received: len(req.Items), Inserted: after - before}, nil
}

// Stats reports queue table counts; Pending uses the same definition the
// batch loop uses.
func (r *Runtime) Stats(ctx context.Context) (QueueStats, error) {
	if r.closed {
		return QueueStats{}, errors.New("translator runtime is closed")
	}
	counts, err := r.queueCounts(ctx)
	if err != nil {
		return QueueStats{}, err
	}
	failed, err := countRows(ctx, r.db, "failed_items")
	if err != nil {
		return QueueStats{}, err
	}
	return QueueStats{Input: counts.input, Pending: counts.pending, Output: counts.output, Failed: failed}, nil
}
