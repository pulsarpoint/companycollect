package main

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"cc-enrich-worker/internal/vec"
)

type refRow struct {
	Code, Label, Division string
	Embedding             []float32
}

type protoRow struct {
	PageType, RootDomain string
	Embedding            []float32
}

// BuildReference normalizes each embedding into the co-ordered NACE matrix.
func BuildReference(rows []refRow) *Reference {
	ref := &Reference{}
	for _, r := range rows {
		ref.Codes = append(ref.Codes, r.Code)
		ref.Labels = append(ref.Labels, r.Label)
		ref.Divisions = append(ref.Divisions, r.Division)
		ref.M = append(ref.M, vec.Norm(r.Embedding))
	}
	return ref
}

// BuildPrototypes normalizes each page-type exemplar embedding.
func BuildPrototypes(rows []protoRow) *Prototypes {
	p := &Prototypes{}
	for _, r := range rows {
		p.Labels = append(p.Labels, r.PageType)
		p.P = append(p.P, vec.Norm(r.Embedding))
	}
	return p
}

// LoadReference reads the NACE matrix and page-type prototypes from ClickHouse.
// Schema is owned by migrations 000044/000045; embeddings are Array(Float32).
func LoadReference(ctx context.Context, conn driver.Conn) (*Reference, *Prototypes, error) {
	var refRows []refRow
	rows, err := conn.Query(ctx,
		"SELECT code,label,division,embedding FROM corpscout.nace_category_embeddings FINAL ORDER BY code")
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var r refRow
		if err := rows.Scan(&r.Code, &r.Label, &r.Division, &r.Embedding); err != nil {
			rows.Close()
			return nil, nil, err
		}
		refRows = append(refRows, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	var protoRows []protoRow
	prows, err := conn.Query(ctx,
		"SELECT page_type,root_domain,embedding FROM corpscout.page_type_exemplars FINAL")
	if err != nil {
		return nil, nil, err
	}
	for prows.Next() {
		var r protoRow
		if err := prows.Scan(&r.PageType, &r.RootDomain, &r.Embedding); err != nil {
			prows.Close()
			return nil, nil, err
		}
		protoRows = append(protoRows, r)
	}
	prows.Close()
	if err := prows.Err(); err != nil {
		return nil, nil, err
	}

	return BuildReference(refRows), BuildPrototypes(protoRows), nil
}
