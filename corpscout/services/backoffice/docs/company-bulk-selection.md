# Swedish company bulk selection

The Info list at `/admin/se/companies` owns a `SeCompanySelection` in its route
component. The header checkbox selects the current page; **Select all N matching
companies** selects the applied query across every page. Unchecking rows in query
mode records exclusions. Pagination and sorting retain the selection. Changing
any applied filter clears query selections; explicit ID selections retain their
existing behavior. Clear removes either selection. Reloads discard selection.

## Adding an action

There is currently no bulk action panel on this list. When adding one, submit the
route's selection directly alongside the action name, for example as JSON:

```ts
{ action: actionName, selection }
```

`selection` is either `{ mode: "ids", companyIds: string[] }` or:

```json
{
  "mode": "query",
  "query": {
    "companyId": "",
    "name": "",
    "status": "active",
    "legalForm": "",
    "entity": "legal",
    "description": "no",
    "source": "esef",
    "datatypes": ["has_financial"]
  },
  "excludedCompanyIds": []
}
```

Use the complete **applied** filters from the loader, including empty fields.
`datatypes` holds the normalized values of the repeated `datatype` URL parameter.
Pagination, sorting, raw SQL and browser-derived lists of all matches do not
belong in the query payload. Selecting all replaces any previous explicit picks.

In the server action, validate the action name and then call
`resolveSeCompanySelection(body.selection)` from
`app/lib/se-company-selection.server.ts`. Pass the returned IDs to that action's
SE company handler. Keep these IDs scoped to Sweden; bare IDs are not globally
unique. The resolver rejects malformed selections and unknown filters rather
than silently expanding the action's target. Surface validation failures as an
invalid request and stop on query errors; never run an action on partial results.

The resolver uses the company's serving table and the exact same filter builder
as the displayed list. It streams matching IDs into a server-side list with no
pagination or row limit. The query is evaluated when the resolver runs, so its
membership can change with the underlying data. The displayed count comes from
the latest list load, minus locally excluded IDs; it is not a persisted snapshot.
For queued actions, resolve once in the worker and persist the resulting targets
for that execution if retries need stable membership.
