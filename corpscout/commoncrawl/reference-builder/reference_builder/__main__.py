"""Rebuild the CommonCrawl reference embeddings against the current endpoint.

    cp ../cc-processor/.env.example ../cc-processor/.env && edit it
    set -a; . ../cc-processor/.env; set +a    # COMMONCRAWL_EMBED_* + CLICKHOUSE_*
    python -m reference_builder

Run from a host that reaches BOTH the embedding endpoint and ClickHouse.
"""
from datetime import datetime, timezone

from .build import ch_client, rebuild_nace, rebuild_page_types
from .embed import EmbeddingClient


def main() -> None:
    embedder = EmbeddingClient.from_env()
    model = embedder.model
    now = datetime.now(timezone.utc)
    run_id = f"rebuild-{now:%Y%m%dT%H%M%SZ}"
    client = ch_client()
    print(f"endpoint model={model}  run_id={run_id}")
    rebuild_nace(client, embedder, model=model, run_id=run_id, now=now)
    rebuild_page_types(client, embedder, model=model, run_id=run_id, now=now)
    print("done — reference tables rebuilt with the current endpoint model.")


if __name__ == "__main__":
    main()
