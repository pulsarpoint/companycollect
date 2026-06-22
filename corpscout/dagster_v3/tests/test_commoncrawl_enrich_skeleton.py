def test_package_imports_and_warcio_available():
    import warcio  # noqa: F401  - dependency must be installed
    import commoncrawl_enrich

    assert commoncrawl_enrich.__version__ == "0.1.0"
