package ariregisterdb

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	isAriregisterSourceExplorerPopulatedSQL = `
SELECT relispopulated
FROM pg_class
WHERE oid = 'ariregister_source.mv_company_explorer'::regclass
`
	refreshAriregisterSourceExplorerSQL             = `REFRESH MATERIALIZED VIEW ariregister_source.mv_company_explorer`
	refreshAriregisterSourceExplorerConcurrentlySQL = `REFRESH MATERIALIZED VIEW CONCURRENTLY ariregister_source.mv_company_explorer`
)

func (g *Gateway) RefreshSourceExplorer(ctx context.Context) (RefreshSourceExplorerResult, error) {
	if g == nil || g.pool == nil {
		return RefreshSourceExplorerResult{}, errors.New("ariregister workflow database pool not available")
	}
	var populated bool
	if err := g.pool.QueryRow(ctx, isAriregisterSourceExplorerPopulatedSQL).Scan(&populated); err != nil {
		return RefreshSourceExplorerResult{}, errors.Wrap(err, "check ariregister source explorer materialized view population")
	}

	refreshSQL := refreshAriregisterSourceExplorerSQL
	if populated {
		refreshSQL = refreshAriregisterSourceExplorerConcurrentlySQL
	}
	if _, err := g.pool.Exec(ctx, refreshSQL); err != nil {
		return RefreshSourceExplorerResult{}, errors.Wrap(err, "refresh ariregister source explorer materialized view")
	}

	summary, err := db.New(g.pool).GetAriregisterSourceCompanyExplorerRefreshSummary(ctx)
	if err != nil {
		return RefreshSourceExplorerResult{}, errors.Wrap(err, "summarize ariregister source explorer materialized view")
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
