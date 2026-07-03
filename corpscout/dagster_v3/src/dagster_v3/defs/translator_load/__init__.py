"""Translator loader assets: anti-join ClickHouse scans that enqueue untranslated
free-text to the Go translator service's bulk endpoint (and insert static-map
columns directly), one asset per source (Norway, Latvia). See ``loader.py``'s
module docstring for the shared scan/enqueue/static-insert contract.
"""
