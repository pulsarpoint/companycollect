package francedb

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (g *Gateway) BeginWorkflowRun(ctx context.Context, params db.BeginFranceWorkflowRunParams) (uuid.UUID, error) {
	if g == nil || g.pool == nil {
		return uuid.Nil, errors.New("france workflow database pool not available")
	}
	params.Metadata = jsonObject(params.Metadata)
	id, err := db.New(g.pool).BeginFranceWorkflowRun(ctx, params)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "begin france workflow run")
	}
	return id, nil
}

func (g *Gateway) FinishWorkflowRun(ctx context.Context, params db.FinishFranceWorkflowRunWithStatsParams) error {
	if g == nil || g.pool == nil {
		return errors.New("france workflow database pool not available")
	}
	if _, err := db.New(g.pool).FinishFranceWorkflowRunWithStats(ctx, params); err != nil {
		return errors.Wrap(err, "finish france workflow run")
	}
	return nil
}

func (g *Gateway) CreateBulkSnapshot(ctx context.Context, params db.CreateFranceBulkSnapshotParams) (uuid.UUID, error) {
	if g == nil || g.pool == nil {
		return uuid.Nil, errors.New("france workflow database pool not available")
	}
	params.Metadata = jsonObject(params.Metadata)
	id, err := db.New(g.pool).CreateFranceBulkSnapshot(ctx, params)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "create france bulk snapshot")
	}
	return id, nil
}

func (g *Gateway) MarkBulkSnapshotParsed(ctx context.Context, params db.MarkFranceBulkSnapshotParsedParams) error {
	if g == nil || g.pool == nil {
		return errors.New("france workflow database pool not available")
	}
	params.Metadata = jsonObject(params.Metadata)
	if err := db.New(g.pool).MarkFranceBulkSnapshotParsed(ctx, params); err != nil {
		return errors.Wrap(err, "mark france bulk snapshot parsed")
	}
	return nil
}

func (g *Gateway) RecordSourceFile(ctx context.Context, params db.RecordFranceSourceFileParams) (uuid.UUID, error) {
	if g == nil || g.pool == nil {
		return uuid.Nil, errors.New("france workflow database pool not available")
	}
	params.Metadata = jsonObject(params.Metadata)
	id, err := db.New(g.pool).RecordFranceSourceFile(ctx, params)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "record france source file")
	}
	return id, nil
}
