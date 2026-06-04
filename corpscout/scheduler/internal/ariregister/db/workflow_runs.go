package ariregisterdb

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (g *Gateway) BeginWorkflowRun(ctx context.Context, params db.BeginAriregisterWorkflowRunParams) (uuid.UUID, error) {
	if g == nil || g.pool == nil {
		return uuid.Nil, errors.New("ariregister workflow database pool not available")
	}
	params.Metadata = jsonObject(params.Metadata)
	id, err := db.New(g.pool).BeginAriregisterWorkflowRun(ctx, params)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "begin ariregister workflow run")
	}
	return id, nil
}

func (g *Gateway) FinishWorkflowRun(ctx context.Context, params db.FinishAriregisterWorkflowRunWithStatsParams) error {
	if g == nil || g.pool == nil {
		return errors.New("ariregister workflow database pool not available")
	}
	if _, err := db.New(g.pool).FinishAriregisterWorkflowRunWithStats(ctx, params); err != nil {
		return errors.Wrap(err, "finish ariregister workflow run")
	}
	return nil
}

func (g *Gateway) CreateBulkSnapshot(ctx context.Context, params db.CreateAriregisterBulkSnapshotParams) (uuid.UUID, error) {
	if g == nil || g.pool == nil {
		return uuid.Nil, errors.New("ariregister workflow database pool not available")
	}
	params.Metadata = jsonObject(params.Metadata)
	id, err := db.New(g.pool).CreateAriregisterBulkSnapshot(ctx, params)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "create ariregister bulk snapshot")
	}
	return id, nil
}

func (g *Gateway) MarkBulkSnapshotParsed(ctx context.Context, params db.MarkAriregisterBulkSnapshotParsedParams) error {
	if g == nil || g.pool == nil {
		return errors.New("ariregister workflow database pool not available")
	}
	params.Metadata = jsonObject(params.Metadata)
	if err := db.New(g.pool).MarkAriregisterBulkSnapshotParsed(ctx, params); err != nil {
		return errors.Wrap(err, "mark ariregister bulk snapshot parsed")
	}
	return nil
}

func (g *Gateway) RecordSourceFile(ctx context.Context, params db.RecordAriregisterSourceFileParams) (uuid.UUID, error) {
	if g == nil || g.pool == nil {
		return uuid.Nil, errors.New("ariregister workflow database pool not available")
	}
	params.Metadata = jsonObject(params.Metadata)
	id, err := db.New(g.pool).RecordAriregisterSourceFile(ctx, params)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "record ariregister source file")
	}
	return id, nil
}
