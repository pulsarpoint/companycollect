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
        from dagster_v3.defs.webtech.client import WebtechApiResource
        from dagster_v3.defs.webtech.storage import parse_webtech_s3_path
        from dagster_v3.defs.webtech.task_assets import build_webtech_task_asset

        destination = parse_webtech_s3_path(self.s3_path)
        task_results = build_webtech_task_asset(destination)
        task_job = dg.define_asset_job("webtech_scan_results_job", selection=dg.AssetSelection.assets(task_results))
        return dg.Definitions(
            assets=[task_results],
            jobs=[task_job],
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
