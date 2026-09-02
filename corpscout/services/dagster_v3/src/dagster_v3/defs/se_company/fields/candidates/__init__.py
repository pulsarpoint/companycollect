"""Candidate extractors for the SE company field registry (spec 2026-09-02, section 5).

One module per source family; each writes rows into corpscout.se_company_field_candidate
through the contract in ``common`` and never touches the published se_company_info row.
"""
