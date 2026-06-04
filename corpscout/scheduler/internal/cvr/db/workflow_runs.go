package cvrdb

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (g *Gateway) BeginWorkflowRun(ctx context.Context, params db.BeginCVRWorkflowRunParams) (uuid.UUID, error) {
	if g == nil || g.pool == nil {
		return uuid.Nil, errors.New("cvr workflow database pool not available")
	}
	params.Metadata = jsonObject(params.Metadata)
	id, err := db.New(g.pool).BeginCVRWorkflowRun(ctx, params)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "begin cvr workflow run")
	}
	return id, nil
}

func (g *Gateway) FinishWorkflowRun(ctx context.Context, params db.FinishCVRWorkflowRunWithStatsParams) error {
	if g == nil || g.pool == nil {
		return errors.New("cvr workflow database pool not available")
	}
	if _, err := db.New(g.pool).FinishCVRWorkflowRunWithStats(ctx, params); err != nil {
		return errors.Wrap(err, "finish cvr workflow run")
	}
	return nil
}

func (g *Gateway) CreateScrollSession(ctx context.Context, params db.CreateCVRScrollSessionParams) (uuid.UUID, error) {
	if g == nil || g.pool == nil {
		return uuid.Nil, errors.New("cvr workflow database pool not available")
	}
	params.Metadata = jsonObject(params.Metadata)
	id, err := db.New(g.pool).CreateCVRScrollSession(ctx, params)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "create cvr scroll session")
	}
	return id, nil
}

func (g *Gateway) FinishScrollSession(ctx context.Context, params db.FinishCVRScrollSessionParams) error {
	if g == nil || g.pool == nil {
		return errors.New("cvr workflow database pool not available")
	}
	params.Metadata = jsonObject(params.Metadata)
	if _, err := db.New(g.pool).FinishCVRScrollSession(ctx, params); err != nil {
		return errors.Wrap(err, "finish cvr scroll session")
	}
	return nil
}
