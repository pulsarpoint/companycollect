module github.com/pulsarpoint/companycollect/companies/united_states

go 1.26.1

require (
	github.com/cockroachdb/errors v1.13.0
	github.com/parquet-go/parquet-go v0.30.1
	github.com/pulsarpoint/companycollect/companies/common v0.0.0
)

replace github.com/pulsarpoint/companycollect/companies/common => ../common
