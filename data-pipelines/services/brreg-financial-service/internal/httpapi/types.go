package httpapi

import (
	"context"

	"github.com/pulsarpoint/brreg-financial-service/internal/models"
)

// LookupService is the interface the handler requires from the service layer.
type LookupService interface {
	Lookup(ctx context.Context, req models.LookupRequest) (models.LookupResponse, error)
}
