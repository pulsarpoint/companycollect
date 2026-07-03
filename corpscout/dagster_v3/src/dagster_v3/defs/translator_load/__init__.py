"""Shared translation-loader library (no assets of its own).

``loader.py`` holds the source-agnostic machinery: anti-join scan SQL
builders, chunked bulk enqueue to the Go translator service, static-map
direct insert, and the stats reachability check. Each source defines its own
translation asset next to its ingest assets (``defs/<source>/translation.py``
or ``defs/<source>/assets/translation.py``), importing these helpers — see
``defs/norway_brreg/assets/translation.py`` (LLM columns + static map) and
``defs/latvia_ur/translation.py`` (LLM column only) as the templates.
"""
