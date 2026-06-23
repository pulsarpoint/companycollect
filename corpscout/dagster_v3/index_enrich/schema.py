"""Parquet schema + writer for the per-domain output (column order == migration 046)."""
import pyarrow as pa

DOMAINS_COLUMNS = (
    "crawl_id", "url", "root_domain", "subdomain", "emails", "email_count",
    "page_type", "page_type_score", "nace_code", "nace_label", "nace_division",
    "nace_confident", "nace_margin", "nace_score", "nace_method",
    "nace_top3_codes", "nace_top3_labels", "nace_top3_scores",
    "source_url", "source_run_id", "resolved_at",
)
DOMAINS_PARQUET_SCHEMA = pa.schema([
    ("crawl_id", pa.string()), ("url", pa.string()), ("root_domain", pa.string()),
    ("subdomain", pa.string()), ("emails", pa.list_(pa.string())), ("email_count", pa.uint32()),
    ("page_type", pa.string()), ("page_type_score", pa.float32()),
    ("nace_code", pa.string()), ("nace_label", pa.string()), ("nace_division", pa.string()),
    ("nace_confident", pa.uint8()), ("nace_margin", pa.float32()), ("nace_score", pa.float32()),
    ("nace_method", pa.string()), ("nace_top3_codes", pa.list_(pa.string())),
    ("nace_top3_labels", pa.list_(pa.string())), ("nace_top3_scores", pa.list_(pa.float32())),
    ("source_url", pa.string()), ("source_run_id", pa.string()),
    ("resolved_at", pa.timestamp("us", tz="UTC")),
])
# per-page technologies (column order == migration 047)
TECHNOLOGIES_COLUMNS = (
    "crawl_id", "url", "root_domain", "subdomain", "technology", "category",
    "version", "confidence", "source_url", "source_run_id", "resolved_at",
)


def write_domain_rows_parquet(rows: list, out_path) -> int:
    import pyarrow.parquet as pq

    columns = list(zip(*rows)) if rows else [() for _ in DOMAINS_COLUMNS]
    arrays = [pa.array(list(col), type=DOMAINS_PARQUET_SCHEMA.field(i).type)
              for i, col in enumerate(columns)]
    pq.write_table(pa.Table.from_arrays(arrays, schema=DOMAINS_PARQUET_SCHEMA), out_path)
    return len(rows)
