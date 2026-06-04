package ariregisterdb

import "strings"

const defaultSourceProfileCopyLimit int32 = 1000

type NormalizeSourceProfilesCommand struct {
	IDs     []string
	Filters map[string]string
	Limit   int32
	Trigger string
}

type NormalizeSourceProfilesResult struct {
	RecordsSeen                  int32
	CompaniesUpserted            int32
	CompanyNamesUpserted         int32
	CompanyStatusesUpserted      int32
	LegalFormsUpserted           int32
	AddressesUpserted            int32
	ContactsUpserted             int32
	WebsitesUpserted             int32
	DomainsUpserted              int32
	IndustriesUpserted           int32
	CapitalUpserted              int32
	FinancialYearPeriodsUpserted int32
	AnnualReportsUpserted        int32
	ArticlesUpserted             int32
	RegistryNotesUpserted        int32
}

type RefreshSourceExplorerResult struct {
	Refreshed             bool
	UsedConcurrentRefresh bool
	SourceEntries         int64
	LatestSourceUpdatedAt *string
}

func textFilter(filters map[string]string, keys ...string) *string {
	if filters == nil {
		return nil
	}
	for _, key := range keys {
		value := strings.TrimSpace(filters[key])
		if value != "" {
			return &value
		}
	}
	return nil
}
