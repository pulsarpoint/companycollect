package main

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ReferenceMeta is the model/dim/coverage of the NACE reference table — used to fail fast at
// startup if the reference was built with a different model than this worker will embed with
// (different model = different vector space = meaningless cosine scores).
type ReferenceMeta struct {
	Model    string
	Dim      uint32
	Count    uint64 // distinct NACE codes that have an embedding
	Expected uint64 // distinct current, non-section NACE codes in the source taxonomy
}

// LoadReferenceMeta reads the reference table's (model, dim) — which must be singular — plus the
// embedded-code count and the expected NACE-category count. The conn must still be open.
func LoadReferenceMeta(ctx context.Context, conn driver.Conn) (ReferenceMeta, error) {
	var m ReferenceMeta
	rows, err := conn.Query(ctx,
		`SELECT embedding_model, embedding_dim FROM corpscout.nace_category_embeddings FINAL
		 GROUP BY embedding_model, embedding_dim`)
	if err != nil {
		return m, fmt.Errorf("query reference meta: %w", err)
	}
	defer rows.Close()
	combos := 0
	for rows.Next() {
		combos++
		if err := rows.Scan(&m.Model, &m.Dim); err != nil {
			return m, err
		}
	}
	if combos == 0 {
		return m, fmt.Errorf("corpscout.nace_category_embeddings is empty — run the reference-builder")
	}
	if combos > 1 {
		return m, fmt.Errorf("corpscout.nace_category_embeddings mixes %d (model,dim) combinations — rebuild it", combos)
	}
	if err := conn.QueryRow(ctx,
		`SELECT uniqExact(code) FROM corpscout.nace_category_embeddings`).Scan(&m.Count); err != nil {
		return m, fmt.Errorf("count embeddings: %w", err)
	}
	if err := conn.QueryRow(ctx,
		`SELECT uniqExact(code) FROM corpscout.nace_categories
		 WHERE is_current = 1 AND level != 'section' AND description_en != ''`).Scan(&m.Expected); err != nil {
		return m, fmt.Errorf("count nace categories: %w", err)
	}
	return m, nil
}

// VerifyReference fails fast unless the reference table (a) has an embedding for every NACE
// category, and (b) was built with the same model + dimensionality this worker is about to embed
// pages with. It probes the live endpoint for the real dimension.
func VerifyReference(meta ReferenceMeta, model string, emb embedderIface) error {
	if meta.Count != meta.Expected {
		return fmt.Errorf("reference has embeddings for %d/%d NACE categories — rebuild corpscout.nace_category_embeddings",
			meta.Count, meta.Expected)
	}
	if model != "" && meta.Model != model {
		return fmt.Errorf("reference built with model=%q but worker is configured for model=%q — rebuild the reference with this model",
			meta.Model, model)
	}
	probe, err := emb.Embed([]string{"industry classification reference check"}, "")
	if err != nil {
		return fmt.Errorf("probe embed: %w", err)
	}
	if len(probe) == 0 || len(probe[0]) == 0 {
		return fmt.Errorf("probe embed returned no vector")
	}
	if uint32(len(probe[0])) != meta.Dim {
		return fmt.Errorf("endpoint produces dim=%d but the reference is dim=%d — model mismatch, rebuild the reference",
			len(probe[0]), meta.Dim)
	}
	return nil
}
