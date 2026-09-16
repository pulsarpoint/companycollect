# Database planning is deferred

The active task is the website crawler:

```text
Input URL → Crawl4AI → page extraction and scored next links
                              ↓
                 final technology classification
                              ↓
                   complete JSON → RustFS/S3
```

Keep original source sections, all research objectives, technology mentions,
classifications, links, provenance, coverage and errors in the submitted JSON.
Link scores feed the crawler queue; each fetched page is analyzed independently.

- [Current crawler design](PAGE_AGENT_DESIGN.md)
- [Technology mention and classification contract](TECHNOLOGY_MENTION_DESIGN.md)
- [JSON submission contract](TECHNOLOGY_MENTION_STORAGE.md)

ClickHouse parsing and storage are deferred. The earlier broader database plan
is retained solely as [historical material](archive/TECHNOLOGY_DB_IMPLEMENTATION_PLAN_20260907.md).
