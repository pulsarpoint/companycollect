package brregdb

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (g *Gateway) ConvertSourceCapitalToUSD(
	ctx context.Context,
	command ConvertSourceCapitalToUSDCommand,
) (ConvertSourceCapitalToUSDResult, error) {
	if g.pool == nil {
		return ConvertSourceCapitalToUSDResult{}, errors.New("brreg workflow database pool not available")
	}
	if command.Limit < 0 {
		return ConvertSourceCapitalToUSDResult{}, errors.New("brreg source capital fx limit cannot be negative")
	}
	trigger := strings.TrimSpace(command.Trigger)
	if trigger == "" {
		trigger = "manual"
	}
	rateDate, err := optionalDate(command.RateDate)
	if err != nil {
		return ConvertSourceCapitalToUSDResult{}, errors.Wrap(err, "parse brreg source capital fx rate date")
	}

	row, err := db.New(g.pool).ConvertBrregSourceCapitalToUSD(ctx, db.ConvertBrregSourceCapitalToUSDParams{
		SelectedIds:        command.IDs,
		Query:              textFilter(command.Filters, "query", "q"),
		LifecycleStatus:    textFilter(command.Filters, "state", "lifecycle_state", "lifecycle_status"),
		RegistrationStatus: textFilter(command.Filters, "registration_status"),
		TranslationStatus:  textFilter(command.Filters, "translation_status"),
		WebsiteStatus:      textFilter(command.Filters, "website_status"),
		Limit:              command.Limit,
		RateDate:           rateDate,
		ForceReprocess:     command.ForceReprocess,
		Trigger:            &trigger,
	})
	if err != nil {
		return ConvertSourceCapitalToUSDResult{}, errors.Wrap(err, "convert brreg source capital to usd")
	}
	return ConvertSourceCapitalToUSDResult{
		CapitalSeen:                    row.CapitalSeen,
		CapitalConverted:               row.CapitalConverted,
		CapitalSkippedMissingRate:      row.CapitalSkippedMissingRate,
		CapitalSkippedAlreadyConverted: row.CapitalSkippedAlreadyConverted,
		RateDate:                       row.RateDate,
	}, nil
}

func optionalDate(value string) (pgtype.Date, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return pgtype.Date{}, nil
	}
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return pgtype.Date{}, err
	}
	return pgtype.Date{Time: parsed, Valid: true}, nil
}
