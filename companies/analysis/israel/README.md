# Company data sources for Israel

## Status

- Official bulk data: **found** (Companies Registrar dataset on `data.gov.il`)
- Official API: **found** (CKAN datastore API)
- Open data portal: **found**
- License: **open** (`other-open` on data.gov.il; confirm exact reuse terms)
- Recommended ingestion path: **datastore API / CSV bulk**

## Best source

The **Companies Registrar company dataset** (`ica_companies`) from the Ministry
of Justice is the best source. It exposes company number, Hebrew name, English
name, corporation type, company status, incorporation date, government-company
flag, limitations, delinquency flag, latest annual report year, address fields,
and status/type/classification codes.

Direct CSV file download returned a challenge page from this environment, but
the CKAN datastore API worked and returned structured sample records.

## Next action

Use the datastore API for first ingestion. If bulk CSV is preferred, test the
download from a normal browser/runtime and fall back to datastore pagination.
