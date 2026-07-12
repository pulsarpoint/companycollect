"""Schema-normalized Common Crawl page-selection inputs."""

from .manifests import SourceSchema


def normalized_source_projection(schema: SourceSchema) -> str:
    detected_mime = (
        "CAST(content_mime_detected AS VARCHAR)"
        if schema.has_content_mime_detected
        else "CAST(NULL AS VARCHAR)"
    )
    languages = (
        "CAST(content_languages AS VARCHAR)"
        if schema.has_content_languages
        else "CAST(NULL AS VARCHAR)"
    )
    return f"""
        CAST(url_host_registered_domain AS VARCHAR) AS root_domain,
        CAST(url_host_name AS VARCHAR) AS url_host_name,
        CAST(url AS VARCHAR) AS url,
        CAST(url_path AS VARCHAR) AS url_path,
        TRY_CAST(fetch_status AS BIGINT) AS fetch_status,
        CAST(content_mime_type AS VARCHAR) AS content_mime_type,
        {detected_mime} AS content_mime_detected,
        {languages} AS content_languages,
        CAST(warc_filename AS VARCHAR) AS warc_filename,
        TRY_CAST(warc_record_offset AS HUGEINT) AS warc_record_offset,
        TRY_CAST(warc_record_length AS HUGEINT) AS warc_record_length
    """.strip()
