"""Shared translation-loader integration (no assets of its own).

``resource.py`` exposes the Go translator HTTP API as an explicit Dagster
resource. ``loader.py`` contains only ClickHouse scan and static-insert SQL
helpers. Source assets keep their scan/enqueue control flow visible.
"""
