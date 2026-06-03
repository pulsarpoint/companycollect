package actions

import (
	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/companydata"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

const defaultTranslationPromptVersion = "v1"

type CompanyTranslationActions struct {
	store      *companydata.Store
	translator *translationclient.Client
}

func NewCompanyTranslationActions(store *companydata.Store, translator *translationclient.Client) *CompanyTranslationActions {
	return &CompanyTranslationActions{store: store, translator: translator}
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
