package brregdb

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)

func (g *Gateway) MapRawRecordIndustriesToNACE(
	ctx context.Context,
	command MapRawRecordIndustriesToNACECommand,
) ([]db.UpsertBrregWorkflowNACEMappingsForRawRecordRow, error) {
	if command.RawRecordID == uuid.Nil {
		return nil, errors.New("raw record id is required")
	}
	if g.pool == nil {
		return nil, errors.New("brreg workflow database pool not available")
	}
	revision := strings.TrimSpace(command.NACERevision)
	if revision == "" {
		revision = nacetaxonomy.DefaultRevision
	}
	rows, err := db.New(g.pool).UpsertBrregWorkflowNACEMappingsForRawRecord(ctx, db.UpsertBrregWorkflowNACEMappingsForRawRecordParams{
		RawRecordID:  command.RawRecordID,
		NaceRevision: revision,
	})
	if err != nil {
		return nil, errors.Wrap(err, "map brreg raw record industries to nace")
	}
	return rows, nil
}

func (g *Gateway) ListRawRecordNACEMappings(ctx context.Context, rawRecordID uuid.UUID) ([]db.BrregWorkflowVNaceMapping, error) {
	if rawRecordID == uuid.Nil {
		return nil, errors.New("raw record id is required")
	}
	if g.pool == nil {
		return nil, errors.New("brreg workflow database pool not available")
	}
	rows, err := db.New(g.pool).ListBrregWorkflowNACEMappingsByRawRecord(ctx, rawRecordID)
	if err != nil {
		return nil, errors.Wrap(err, "list brreg raw record nace mappings")
	}
	return rows, nil
}
