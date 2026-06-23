# Jordan investigation

## Conclusion

Jordan is worth keeping on the backlog, but it should not be treated as a strong
full-register source from this pass. The open portal has useful business-related
datasets, but the entity-level data found is for individual institutions and
contains proprietor names.

## Evidence

- CKAN search for commercial registry found a "commercial registry" CSV, but the
  observed sample was aggregate year/count data.
- CKAN search for institutions found "Table of individual institutions 2", which
  is entity-level and about 6.4 MB.
- A bounded CSV sample and normalized JSONL sample were saved.

## Recommended ingestion

Treat Jordan as a partial source. If implemented, build a clear distinction
between legal entities and individual institutions and avoid publishing proprietor
personal data unless there is a reviewed basis.
