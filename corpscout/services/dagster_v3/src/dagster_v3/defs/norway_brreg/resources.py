from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Any, Protocol
from typing import TYPE_CHECKING

import dagster as dg
import requests
from pydantic import PrivateAttr

from dagster_v3.defs.norway_brreg import entity_records
from dagster_v3.defs.norway_brreg import tables

if TYPE_CHECKING:
    from dagster_aws.s3 import S3Resource

COUNTRY = "NO"
ENTITY_SOURCE_SLUG = "norway_brregenhet"
BRREG_BASE_URL = "https://data.brreg.no/enhetsregisteret/api"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
ENTITY_PROGRESS_LOG_EVERY_ROWS = 1000
UPDATE_MAX_RESULT_WINDOW = 10_000
LOGGER = logging.getLogger(__name__)

BRREG_ENTITIES_COLUMNS = tables.BRREG_ENTITIES_COLUMNS

BRREG_LEGAL_FORM_DESCRIPTION_EN_BY_CODE = {
    "ANS": "General partnership",
    "AS": "Private limited company",
    "ASA": "Public limited company",
    "DA": "Partnership with shared liability",
    "ENK": "Sole proprietorship",
    "FKF": "Municipal enterprise",
    "FORE": "Association",
    "KOMM": "Municipality",
    "NUF": "Norwegian branch of foreign company",
    "SA": "Cooperative",
    "STI": "Foundation",
}


class HttpSession(Protocol):
    headers: dict[str, str]

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        timeout: int,
        stream: bool = False,
    ) -> Any: ...


class NorwayBrregApiResource(dg.ConfigurableResource):
    base_url: str = BRREG_BASE_URL
    user_agent: str = DEFAULT_USER_AGENT
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    update_page_size: int = 10_000

    _session: HttpSession | None = PrivateAttr(default=None)

    def __init__(self, session: HttpSession | None = None, **data: Any) -> None:
        super().__init__(**data)
        self._session = session

    def session(self) -> HttpSession:
        if self._session is None:
            session = requests.Session()
            session.headers["User-Agent"] = self.user_agent
            self._session = session
        return self._session

    def entries_snapshot(
        self,
        *,
        s3: S3Resource,
        bucket: str,
        key: str,
        log: Callable[..., None] | None = None,
    ) -> dict[str, Any]:
        return self._download_entries_snapshot(
            url=f"{self.base_url}/enheter/lastned",
            s3=s3,
            bucket=bucket,
            key=key,
            log=log,
        )

    def entries_snapshot_csv(
        self,
        *,
        s3: S3Resource,
        bucket: str,
        key: str,
        log: Callable[..., None] | None = None,
    ) -> dict[str, Any]:
        return self._download_entries_snapshot(
            url=f"{self.base_url}/enheter/lastned/csv",
            s3=s3,
            bucket=bucket,
            key=key,
            log=log,
        )

    def _download_entries_snapshot(
        self,
        *,
        url: str,
        s3: S3Resource,
        bucket: str,
        key: str,
        log: Callable[..., None] | None,
    ) -> dict[str, Any]:
        s3_client = s3.get_client()
        progress_log = log or LOGGER.info
        try:
            existing_object = s3_client.head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            if _error_code(exc) not in {"404", "NoSuchBucket", "NoSuchKey", "NotFound"}:
                raise
        else:
            bytes_downloaded = (
                existing_object.get("ContentLength")
                if isinstance(existing_object, Mapping)
                else None
            )
            progress_log(
                "Norway Brreg entries snapshot already exists on S3: bucket=%s key=%s bytes=%s",
                bucket,
                key,
                bytes_downloaded,
            )
            return {
                "s3_bucket": bucket,
                "s3_key": key,
                "downloaded": False,
                "bytes_downloaded": bytes_downloaded,
            }

        progress_log(
            "Downloading Norway Brreg entries snapshot to S3: bucket=%s key=%s",
            bucket,
            key,
        )
        response = self.session().get(
            url,
            timeout=self.timeout_seconds,
            stream=True,
        )
        response.raise_for_status()
        s3_client.upload_fileobj(response.raw, bucket, key)
        uploaded_object = s3_client.head_object(Bucket=bucket, Key=key)
        bytes_downloaded = (
            uploaded_object.get("ContentLength")
            if isinstance(uploaded_object, Mapping)
            else None
        )
        progress_log(
            "Downloaded Norway Brreg entries snapshot to S3: bucket=%s key=%s bytes=%s",
            bucket,
            key,
            bytes_downloaded,
        )
        return {
            "s3_bucket": bucket,
            "s3_key": key,
            "downloaded": True,
            "bytes_downloaded": bytes_downloaded,
        }

    def iter_updated_entities(
        self,
        *,
        start: str,
        end: str,
        include_changes: bool = False,
        log: Callable[..., None] | None = None,
        progress_every_rows: int = ENTITY_PROGRESS_LOG_EVERY_ROWS,
    ) -> Iterator[dict[str, Any]]:
        progress_log = log or LOGGER.info
        row_count = 0
        hydrated_row_count = 0
        seen_update_keys: set[str] = set()
        progress_log(
            "Loading Norway Brreg entity updates: updated_at=%s..%s include_changes=%s",
            start,
            end,
            include_changes,
        )
        for update in self._iter_updated_entity_updates_window(
            start=start,
            end=end,
            include_changes=include_changes,
            log=progress_log,
            depth=0,
        ):
            update_key = _entity_update_key(update)
            if update_key in seen_update_keys:
                continue
            seen_update_keys.add(update_key)
            change_type_override = None
            if entity_records.entity_update_requires_hydration(update):
                org_number = _string(update.get("organisasjonsnummer"))
                try:
                    entity = self.get_entity(org_number)
                    hydrated_row_count += 1
                    if (
                        progress_every_rows > 0
                        and hydrated_row_count % progress_every_rows == 0
                    ):
                        progress_log(
                            "Hydrated Norway Brreg entity update rows: rows=%s",
                            hydrated_row_count,
                        )
                except Exception as exc:
                    if not _exception_has_http_status(exc, 410):
                        raise
                    progress_log(
                        "Norway Brreg entity update hydration returned 410; treating org as removed: org_number=%s update_id=%s",
                        org_number,
                        update.get("oppdateringsid"),
                    )
                    entity = None
                    change_type_override = entity_records.ENTITY_CHANGE_TYPE_REMOVED
            else:
                entity = None
            row_count += 1
            if progress_every_rows > 0 and row_count % progress_every_rows == 0:
                progress_log(
                    "Processed Norway Brreg entity update rows: rows=%s", row_count
                )
            yield entity_records.updated_entity_record(
                update,
                entity=entity,
                change_type_override=change_type_override,
            )
        progress_log(
            "Completed Norway Brreg entity updates load: rows=%s hydrated_rows=%s",
            row_count,
            hydrated_row_count,
        )

    def _iter_updated_entity_updates_window(
        self,
        *,
        start: str,
        end: str,
        include_changes: bool,
        log: Callable[..., None],
        depth: int,
    ) -> Iterator[dict[str, Any]]:
        page_number = 0
        page_size = _brreg_update_page_size(self.update_page_size)
        while True:
            params: dict[str, Any] = {
                "dato": start,
                "updatedBefore": end,
                "size": page_size,
                "page": page_number,
                "sort": "id,ASC",
            }
            if include_changes:
                params["includeChanges"] = "true"

            log(
                "Requesting Norway Brreg entity updates page: window=%s..%s page=%s size=%s",
                start,
                end,
                page_number,
                page_size,
            )
            payload = self._get_json(
                f"{self.base_url}/oppdateringer/enheter",
                params=params,
            )
            total_elements = _update_payload_total_elements(payload)
            if page_number == 0 and total_elements > UPDATE_MAX_RESULT_WINDOW:
                left_start, left_end, right_start, right_end = _split_update_window(
                    start, end
                )
                log(
                    "Splitting Norway Brreg entity updates window: window=%s..%s "
                    "total_elements=%s max_result_window=%s left=%s..%s right=%s..%s",
                    start,
                    end,
                    total_elements,
                    UPDATE_MAX_RESULT_WINDOW,
                    left_start,
                    left_end,
                    right_start,
                    right_end,
                )
                yield from self._iter_updated_entity_updates_window(
                    start=left_start,
                    end=left_end,
                    include_changes=include_changes,
                    log=log,
                    depth=depth + 1,
                )
                yield from self._iter_updated_entity_updates_window(
                    start=right_start,
                    end=right_end,
                    include_changes=include_changes,
                    log=log,
                    depth=depth + 1,
                )
                return

            updates = entity_records.entity_updates_from_payload(payload)
            log(
                "Norway Brreg entity updates page loaded: window=%s..%s page=%s "
                "updates=%s total_elements=%s",
                start,
                end,
                page_number,
                len(updates),
                total_elements,
            )
            yield from updates

            if not entity_records.update_payload_has_next_page(
                payload, current_page=page_number
            ):
                break
            page_number += 1
            if page_size * (page_number + 1) > UPDATE_MAX_RESULT_WINDOW:
                raise ValueError(
                    "Norway Brreg entity updates pagination exceeded source result "
                    f"window: window={start}..{end} page={page_number} size={page_size}"
                )

    def get_entity(self, org_number: str) -> dict[str, Any]:
        payload = self._get_json(f"{self.base_url}/enheter/{org_number}")
        if not isinstance(payload, dict):
            raise ValueError(
                f"Expected Brreg entity payload to be an object: {org_number}"
            )
        return payload

    def _get_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        response = self.session().get(
            url,
            params=params,
            timeout=self.timeout_seconds,
        )
        _raise_for_status(response)
        return response.json()


def build_entity_rows(
    entities: list[dict[str, Any]], *, run_id: str
) -> list[dict[str, Any]]:
    return [
        _entity_row(entity, line_number=index, run_id=run_id)
        for index, entity in enumerate(entities, start=1)
    ]


def source_payload_hash(payload: dict[str, Any]) -> str:
    body = _json_dumps(payload, sort_keys=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def norway_legal_form_description_en(code: str) -> str:
    return BRREG_LEGAL_FORM_DESCRIPTION_EN_BY_CODE.get(code.upper(), "")


def _entity_row(
    entity: dict[str, Any], *, line_number: int, run_id: str
) -> dict[str, Any]:
    org_number = _string(entity.get("organisasjonsnummer"))
    vat_registered = _bool(entity.get("registrertIMvaregisteret"))
    business_address = _dict(entity.get("forretningsadresse"))
    postal_address = _dict(entity.get("postadresse"))
    legal_form = _dict(entity.get("organisasjonsform"))
    nace1 = _dict(entity.get("naeringskode1"))
    nace2 = _dict(entity.get("naeringskode2"))
    nace3 = _dict(entity.get("naeringskode3"))
    status = _entity_status(entity)
    legal_form_description_original = _string(legal_form.get("beskrivelse"))
    legal_form_code = _string(legal_form.get("kode"))
    nace1_description_original = _string(nace1.get("beskrivelse"))
    nace2_description_original = _string(nace2.get("beskrivelse"))
    nace3_description_original = _string(nace3.get("beskrivelse"))
    articles_purpose_original = _joined_text_lines(entity.get("vedtektsfestetFormaal"))
    activity_text_original = _joined_text_lines(entity.get("aktivitet"))
    return {
        "country_iso2": COUNTRY,
        "source_slug": ENTITY_SOURCE_SLUG,
        "source_run_id": run_id,
        "source_line_number": line_number,
        "source_record_id": org_number,
        "source_payload_hash": source_payload_hash(entity),
        "org_number": org_number,
        "vat_id": f"NO{org_number}MVA" if vat_registered and org_number else "",
        "legal_name": _string(entity.get("navn")),
        "legal_form_code": legal_form_code,
        "legal_form_description_original": legal_form_description_original,
        "legal_form_description_en": norway_legal_form_description_en(legal_form_code),
        "registration_date": _string(entity.get("registreringsdatoEnhetsregisteret")),
        "incorporation_date": _string(entity.get("stiftelsesdato")),
        "website": _string(entity.get("hjemmeside")),
        "email": _string(entity.get("epostadresse")),
        "phone": _string(entity.get("telefon")),
        "mobile": _string(entity.get("mobil")),
        "nace1_code": _string(nace1.get("kode")),
        "nace1_description_original": nace1_description_original,
        "nace1_description_en": "",
        "nace2_code": _string(nace2.get("kode")),
        "nace2_description_original": nace2_description_original,
        "nace2_description_en": "",
        "nace3_code": _string(nace3.get("kode")),
        "nace3_description_original": nace3_description_original,
        "nace3_description_en": "",
        "articles_purpose_original": articles_purpose_original,
        "articles_purpose_en": "",
        "activity_text_original": activity_text_original,
        "activity_text_en": "",
        "employee_count": _int_or_none(entity.get("antallAnsatte")),
        "has_registered_employee_count": _bool(
            entity.get("harRegistrertAntallAnsatte")
        ),
        "business_address_lines": _address_lines(business_address),
        "business_postal_code": _string(business_address.get("postnummer")),
        "business_city": _string(business_address.get("poststed")),
        "business_municipality": _string(business_address.get("kommune")),
        "business_municipality_code": _string(business_address.get("kommunenummer")),
        "business_country": _string(business_address.get("land")),
        "business_country_code": _string(business_address.get("landkode")),
        "postal_address_lines": _address_lines(postal_address),
        "postal_postal_code": _string(postal_address.get("postnummer")),
        "postal_city": _string(postal_address.get("poststed")),
        "postal_municipality": _string(postal_address.get("kommune")),
        "postal_municipality_code": _string(postal_address.get("kommunenummer")),
        "postal_country": _string(postal_address.get("land")),
        "postal_country_code": _string(postal_address.get("landkode")),
        "is_vat_registered": vat_registered,
        "is_enterprise_register_registered": _bool(
            entity.get("registrertIForetaksregisteret")
        ),
        "is_group_member": _bool(entity.get("erIKonsern")),
        "parent_org_number": _string(entity.get("overordnetEnhet")),
        "last_submitted_accounts_year": _string(
            entity.get("sisteInnsendteAarsregnskap")
        ),
        "status": status,
        "is_active": status == "active",
        "source_url": _source_url(entity),
        "raw_entity": _json_dumps(entity),
    }


def _raise_for_status(response: Any) -> None:
    raise_for_status = getattr(response, "raise_for_status", None)
    if callable(raise_for_status):
        raise_for_status()
        return
    status_code = getattr(response, "status_code", 200)
    if status_code >= 400:
        raise RuntimeError(f"HTTP {status_code}")


def _exception_has_http_status(exc: Exception, status_code: int) -> bool:
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None) == status_code


def _error_code(exc: Exception) -> str:
    response = getattr(exc, "response", {})
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    return str(error.get("Code", ""))


def _brreg_update_page_size(configured_page_size: int) -> int:
    return max(1, min(configured_page_size, UPDATE_MAX_RESULT_WINDOW))


def _update_payload_total_elements(payload: dict[str, Any]) -> int:
    page = payload.get("page")
    if not isinstance(page, dict):
        return 0
    total_elements = page.get("totalElements")
    return total_elements if isinstance(total_elements, int) else 0


def _split_update_window(start: str, end: str) -> tuple[str, str, str, str]:
    start_time = _parse_brreg_timestamp(start)
    end_time = _parse_brreg_timestamp(end)
    if end_time <= start_time:
        raise ValueError(
            f"Cannot split empty Norway Brreg update window: {start}..{end}"
        )
    if end_time - start_time <= timedelta(milliseconds=1):
        raise ValueError(
            "Cannot split Norway Brreg update window below 1 millisecond while "
            f"respecting the source result window: {start}..{end}"
        )
    midpoint = start_time + ((end_time - start_time) / 2)
    midpoint_text = _format_brreg_timestamp(midpoint)
    return start, midpoint_text, midpoint_text, end


def _parse_brreg_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    timestamp = datetime.fromisoformat(normalized)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def _format_brreg_timestamp(value: datetime) -> str:
    return (
        value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


def _entity_update_key(update: dict[str, Any]) -> str:
    update_id = update.get("oppdateringsid")
    if update_id is not None:
        return f"id:{update_id}"
    return (
        f"fallback:{_string(update.get('organisasjonsnummer'))}:"
        f"{_string(update.get('dato'))}:{_string(update.get('endringstype'))}"
    )


def _entity_status(entity: dict[str, Any]) -> str:
    if _bool(entity.get("konkurs")):
        return "bankrupt"
    if _bool(entity.get("underTvangsavviklingEllerTvangsopplosning")):
        return "compulsory_liquidation"
    if _bool(entity.get("underAvvikling")):
        return "liquidation"
    return "active"


def _source_url(entity: dict[str, Any]) -> str:
    links = _dict(entity.get("_links"))
    self_link = _dict(links.get("self"))
    return _string(self_link.get("href"))


def _json_dumps(payload: dict[str, Any], *, sort_keys: bool = False) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=sort_keys,
        default=_json_default,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            return str(value)
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _address_lines(address: dict[str, Any]) -> str:
    return "\n".join(
        _string(line) for line in _list(address.get("adresse")) if _string(line)
    )


def _joined_text_lines(value: Any) -> str:
    return "\n".join(_string(line) for line in _list(value) if _string(line))


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _bool(value: Any) -> bool:
    return bool(value)


def _int_or_none(value: Any) -> int | None:
    return value if isinstance(value, int) else None


def _string(value: Any) -> str:
    return "" if value is None else str(value)
