import json
from typing import Any

from botocore.exceptions import ClientError

from crawler_ratsit.models import identity_sha256


class RatsitObjectStore:
    def __init__(
        self,
        client: Any,
        *,
        bucket: str,
        prefix: str,
    ) -> None:
        self._client = client
        self.bucket = bucket
        self._prefix = prefix.strip("/")

    def response_key(self, *, company_id: str, batch_id: str) -> str:
        return (
            f"{self._prefix}/batch_id={batch_id}/"
            f"identity_sha256={identity_sha256(company_id)}/response.json"
        )

    def read_json_if_exists(self, key: str) -> dict[str, Any] | None:
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if _error_code(error) in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

        payload = json.loads(response["Body"].read())
        if not isinstance(payload, dict):
            raise TypeError(f"S3 object {key} must contain a JSON object")
        return payload

    def write_json_if_absent(self, key: str, value: dict[str, Any]) -> bool:
        body = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        try:
            self._client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                IfNoneMatch="*",
            )
        except ClientError as error:
            if _error_code(error) in {"PreconditionFailed", "412"}:
                return False
            raise
        return True


def _error_code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", ""))
