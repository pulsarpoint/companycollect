package brregdb

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	isSourceExplorerPopulatedSQL = `
SELECT relispopulated
FROM pg_class
WHERE oid = 'brreg_source.mv_company_explorer'::regclass
`
	refreshSourceExplorerSQL             = `REFRESH MATERIALIZED VIEW brreg_source.mv_company_explorer`
	refreshSourceExplorerConcurrentlySQL = `REFRESH MATERIALIZED VIEW CONCURRENTLY brreg_source.mv_company_explorer`
)

func (g *Gateway) RefreshSourceExplorer(ctx context.Context) (RefreshSourceExplorerResult, error) {
	if g.pool == nil {
		return RefreshSourceExplorerResult{}, errors.New("brreg workflow database pool not available")
	}

	var populated bool
	if err := g.pool.QueryRow(ctx, isSourceExplorerPopulatedSQL).Scan(&populated); err != nil {
		return RefreshSourceExplorerResult{}, errors.Wrap(err, "check brreg source explorer materialized view population")
	}

	refreshSQL := refreshSourceExplorerSQL
	if populated {
		refreshSQL = refreshSourceExplorerConcurrentlySQL
	}
	if _, err := g.pool.Exec(ctx, refreshSQL); err != nil {
		return RefreshSourceExplorerResult{}, errors.Wrap(err, "refresh brreg source explorer materialized view")
	}

	summary, err := db.New(g.pool).GetBrregSourceCompanyExplorerRefreshSummary(ctx)
	if err != nil {
		return RefreshSourceExplorerResult{}, errors.Wrap(err, "summarize brreg source explorer materialized view")
	}
	var latest *string
	if value := strings.TrimSpace(summary.LatestSourceUpdatedAt); value != "" {
		latest = &value
	}
	return RefreshSourceExplorerResult{
		Refreshed:             true,
		UsedConcurrentRefresh: populated,
		SourceEntries:         summary.SourceEntries,
		LatestSourceUpdatedAt: latest,
	}, nil
}
