import dagster as dg


class WebtechScannerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materialize the remote Webtech scan and its durable result index.

    The component owns only orchestration configuration. Browser capacity,
    timeouts, extension files, and RustFS credentials remain on the scanner
    workstation.
    """

    api_url: str
    s3_path: str

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        del context
        from dagster_v3.defs.common.resources import ObjectStoreResource
        from dagster_v3.defs.webtech.assets import (
            build_webtech_assets,
            build_webtech_jobs,
        )
        from dagster_v3.defs.webtech.client import WebtechApiResource
        from dagster_v3.defs.webtech.storage import parse_webtech_s3_path

        destination = parse_webtech_s3_path(self.s3_path)
        assets = build_webtech_assets(destination)
        scan_job, finalize_job = build_webtech_jobs(assets)
        return dg.Definitions(
            assets=[*assets],
            jobs=[scan_job, finalize_job],
            resources={
                "webtech_api": WebtechApiResource(
                    base_url=self.api_url,
                    api_token=dg.EnvVar("WEBTECH_API_TOKEN"),
                ),
                "webtech_object_store": ObjectStoreResource(
                    bucket=destination.bucket,
                ),
            },
        )
