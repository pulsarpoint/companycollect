# Peru investigation

## Conclusion

Peru is a high-priority source because SUNAT publishes a national RUC bulk file.
The main blocker for discovery is only file size, not access.

## Evidence

- SUNAT download page returned HTTP 200 and was saved.
- `padron_reducido_RUC.zip` returned HTTP 200, `Content-Type: application/zip`,
  `Content-Length: 388334682`, and `Last-Modified: 2026-06-22`.
- The local-anexo ZIP returned HTTP 200 and `Content-Length: 13796901`.

## Recommended ingestion

Use a snapshot-style bulk job with explicit size logging, checksum calculation,
and unzip/profile. Start with the 13 MB local-anexo file if a small parser spike
is useful, then fetch the 370 MB main RUC file.
