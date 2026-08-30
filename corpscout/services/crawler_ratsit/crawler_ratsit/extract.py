import asyncio
import json
import logging
import math
import os
import re
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import click
from lxml import html
from lxml.html import HtmlElement
from playwright.async_api import BrowserContext
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from crawler_ratsit.browser import launch_browser_context
from crawler_ratsit.config import BrowserSettings, ProcessSettings

LOGGER = logging.getLogger(__name__)

RATSIT_HOSTS = frozenset({"ratsit.se", "www.ratsit.se"})
PAGE_CONTENT_SELECTOR = "main .main-inner"
DEFAULT_PAGE_TIMEOUT_MS = 60_000

type JsonObject = dict[str, object]

FINANCIAL_TABLE_CATEGORIES = {
    "Nyckeltal": "key_ratios",
    "Bokslutsperiod": "fiscal_period",
    "Resultaträkning": "income_statement",
    "Balansräkning": "balance_sheet",
}
FINANCIAL_METRIC_KEYS = {
    "income_statement": {
        "Omsättning": "revenue",
        "Rörelsens omsättning": "revenue",
        "Rörelsens kostnader": "operating_costs",
        "Rörelseresultat": "operating_profit",
        "Resultat efter finansnetto": "profit_after_financial_items",
        "Årets resultat": "net_income",
    },
    "balance_sheet": {
        "Omsättningstillgångar": "current_assets",
        "Anläggningstillgångar": "fixed_assets",
        "Aktiekapital": "share_capital",
        "Eget kapital": "equity",
        "Obeskattade reserver": "untaxed_reserves",
        "Avsättningar": "provisions",
        "Långfristiga skulder": "long_term_liabilities",
        "Kortfristiga skulder": "current_liabilities",
        "Skulder": "liabilities",
        "Tillgångar": "total_assets",
        "Balansomslutning": "balance_sheet_total",
    },
    "key_ratios": {
        "Kassalikviditet (%)": "cash_liquidity_percent",
        "Soliditet (%)": "equity_ratio_percent",
        "Vinstmarginal (%)": "net_profit_margin_percent",
        "Vinstmarginal efter finansnetto (%)": "net_profit_margin_percent",
        "EBITDA": "ebitda",
        "Personalkostnad per anställd (MSEK)": ("personnel_cost_per_employee_msek"),
        "Omsättning per anställd": "revenue_per_employee_msek",
        "Omsättning per anställd (MSEK)": "revenue_per_employee_msek",
        "Omsättningsförändring (%)": "revenue_change_percent",
        "Genomsnittlig lön": "average_salary",
    },
}


class RatsitExtractionError(RuntimeError):
    pass


def validate_ratsit_url(url: str) -> str:
    normalized_url = url.strip()
    if not normalized_url:
        raise ValueError("Ratsit URL must not be blank")

    parsed_url = urlsplit(normalized_url)
    if parsed_url.scheme != "https":
        raise ValueError("Ratsit URL must use HTTPS")
    if parsed_url.hostname not in RATSIT_HOSTS:
        raise ValueError("Ratsit URL must use ratsit.se or www.ratsit.se")
    if parsed_url.username is not None or parsed_url.password is not None:
        raise ValueError("Ratsit URL must not contain credentials")
    if parsed_url.port not in {None, 443}:
        raise ValueError("Ratsit URL must not use a non-standard port")
    if parsed_url.path in {"", "/"}:
        raise ValueError("Ratsit URL must identify a report page")
    return normalized_url


def parse_company_page(page_html: str, *, source_url: str) -> JsonObject:
    document = _parse_html(page_html)
    json_ld_items = _json_ld_items(document)
    organization = _json_ld_item(json_ld_items, "Organization")
    article = _json_ld_item(json_ld_items, "Article")
    coordinates = _json_ld_item(json_ld_items, "GeoCoordinates")
    address = _mapping_value(organization, "address")

    company = {
        "name": _string_value(organization, "name")
        or _first_text(
            document,
            "//main//aside[contains(concat(' ', normalize-space(@class), ' '), "
            "' quick-facts ')]//h1[1]",
        ),
        "organization_number": _company_field(
            document,
            section="Juridisk Person",
            label="Organisationsnummer",
        ),
        "legal_form": _company_field(
            document,
            section="Juridisk Person",
            label="Bolagsform",
        ),
        "status": _company_field(
            document,
            section="Juridisk Person",
            label="Status",
        ),
        "address": {
            "street": _string_value(address, "streetAddress")
            or _company_field(document, section="Adress", label="Gatuadress"),
            "postal_code": _string_value(address, "postalCode")
            or _contact_field(document, "Postnummer"),
            "locality": _string_value(address, "addressLocality")
            or _contact_field(document, "Postort"),
            "county": _contact_field(document, "Län"),
        },
        "industry_codes": _industry_codes(document),
        "business_description": _first_text(
            document,
            "//*[@id='foretaget']//h3[normalize-space()='Verksamhetsbeskrivning']"
            "/following-sibling::p[1]",
        ),
        "summary": _all_text(
            document,
            "//h2[starts-with(normalize-space(.), 'Mer om ')]"
            "/ancestor::div[contains(concat(' ', normalize-space(@class), ' '), "
            "' row ')][1]"
            "/following-sibling::div[contains(concat(' ', normalize-space(@class), "
            "' '), ' row ')][1]//p[normalize-space()]",
        ),
    }

    return {
        "company": company,
        "responsible_people": _responsible_people(document, source_url=source_url),
        "workplaces": _workplaces(document, json_ld_items=json_ld_items),
        "financials": _financial_reports(document),
        "people_at_address": _people_at_address(
            document,
            source_url=source_url,
        ),
        "coordinates": {
            "latitude": _number_value(coordinates, "latitude"),
            "longitude": _number_value(coordinates, "longitude"),
        },
        "source_url": source_url,
        "date_modified": _string_value(article, "dateModified"),
    }


def parse_people_at_address(page_html: str, *, source_url: str) -> list[JsonObject]:
    return _people_at_address(_parse_html(page_html), source_url=source_url)


async def extract_ratsit_url(
    url: str,
    *,
    process_settings: ProcessSettings,
    browser_id: str,
    environment: Mapping[str, str],
    headless: bool | None,
    follow_people: bool,
) -> JsonObject:
    requested_url = validate_ratsit_url(url)
    browser_settings = _browser_settings(process_settings, browser_id)
    context = await launch_browser_context(
        browser_settings,
        profile_directory=(
            process_settings.state_directory / "standalone-extractor" / browser_id
        ),
        license_key=environment.get("CLOAKBROWSER_LICENSE_KEY", "").strip() or None,
        headless=process_settings.headless if headless is None else headless,
    )
    try:
        final_url, page_html = await _fetch_rendered_html(
            context,
            requested_url,
            timeout_ms=_page_timeout_ms(environment),
        )
        report = parse_company_page(page_html, source_url=final_url)
        if follow_people and not report["people_at_address"]:
            profile_url = _first_responsible_profile_url(report)
            if profile_url is not None:
                person_url, person_html = await _fetch_rendered_html(
                    context,
                    validate_ratsit_url(profile_url),
                    timeout_ms=_page_timeout_ms(environment),
                )
                report["people_at_address"] = parse_people_at_address(
                    person_html,
                    source_url=person_url,
                )
        return report
    finally:
        await context.close()


async def _fetch_rendered_html(
    context: BrowserContext,
    url: str,
    *,
    timeout_ms: int,
) -> tuple[str, str]:
    page = await context.new_page()
    try:
        response = await page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )
        if response is None:
            raise RatsitExtractionError("Ratsit navigation returned no HTTP response")
        if response.status == 429:
            raise RatsitExtractionError("Ratsit returned HTTP 429 (rate limited)")
        if not response.ok:
            raise RatsitExtractionError(
                f"Ratsit returned HTTP status {response.status}"
            )

        try:
            await page.locator(PAGE_CONTENT_SELECTOR).first.wait_for(
                state="visible",
                timeout=timeout_ms,
            )
        except PlaywrightTimeoutError as error:
            raise RatsitExtractionError(
                f"Ratsit content selector was not visible: {PAGE_CONTENT_SELECTOR}"
            ) from error
        validate_ratsit_url(page.url)
        return page.url, await page.content()
    finally:
        await page.close()


def _parse_html(page_html: str) -> HtmlElement:
    if not page_html.strip():
        raise ValueError("Ratsit HTML must not be blank")
    try:
        return html.fromstring(page_html)
    except (TypeError, ValueError) as error:
        raise ValueError(f"cannot parse Ratsit HTML: {error}") from error


def _json_ld_items(document: HtmlElement) -> list[Mapping[str, object]]:
    items: list[Mapping[str, object]] = []
    for script_text in document.xpath("//script[@type='application/ld+json']/text()"):
        if not isinstance(script_text, str) or not script_text.strip():
            continue
        try:
            value = json.loads(script_text)
        except json.JSONDecodeError as error:
            LOGGER.warning("ignoring invalid Ratsit JSON-LD: %s", error)
            continue

        values = value if isinstance(value, list) else [value]
        items.extend(item for item in values if isinstance(item, dict))
    return items


def _json_ld_item(
    items: list[Mapping[str, object]],
    item_type: str,
) -> Mapping[str, object] | None:
    return next((item for item in items if item.get("@type") == item_type), None)


def _company_field(
    document: HtmlElement,
    *,
    section: str,
    label: str,
) -> str | None:
    return _first_text(
        document,
        "//*[@id='foretaget']//h3[normalize-space()=$section]"
        "/following-sibling::div[div[1][normalize-space()=$label]][1]/div[2]",
        section=section,
        label=f"{label}:",
    )


def _contact_field(document: HtmlElement, label: str) -> str | None:
    return _first_text(
        document,
        "//table[.//thead//*[normalize-space()='Addressuppgifter']]"
        "//tr[td[1][normalize-space()=$label]]/td[2]",
        label=f"{label}:",
    )


def _industry_codes(document: HtmlElement) -> list[JsonObject]:
    rows = _all_text(
        document,
        "//*[@id='foretaget']//p[preceding-sibling::h3[1][contains("
        "normalize-space(), 'Svensk näringsgrensindelning')]]",
    )
    industry_codes: list[JsonObject] = []
    for row in rows:
        code, separator, description = row.partition(" - ")
        industry_codes.append(
            {
                "code": code or None,
                "description": description if separator else None,
            }
        )
    return industry_codes


def _financial_reports(document: HtmlElement) -> list[JsonObject]:
    periods_by_report: dict[int, dict[int, JsonObject]] = {}
    units_by_report: dict[int, str] = {}
    category_occurrences: dict[str, int] = {}

    for table in _financial_tables(document):
        header_cells = _financial_header_cells(table)
        if len(header_cells) < 2:
            continue
        source_category = _financial_label(header_cells[0])
        category = _financial_category(source_category)
        if category is None:
            continue

        report_index = category_occurrences.get(category, 0)
        category_occurrences[category] = report_index + 1
        years = [_financial_year(cell) for cell in header_cells[1:]]
        if any(year is None for year in years):
            continue
        fiscal_years = [year for year in years if year is not None]
        report_periods = periods_by_report.setdefault(report_index, {})
        _apply_financial_table(
            table,
            category=category,
            fiscal_years=fiscal_years,
            periods=report_periods,
        )

        monetary_unit = _monetary_unit(source_category)
        if monetary_unit is not None:
            units_by_report[report_index] = monetary_unit

    detail_sections = _latest_financial_details(document)
    for report_index, (monetary_unit, period) in enumerate(detail_sections):
        report_periods = periods_by_report.setdefault(report_index, {})
        fiscal_year = period["fiscal_year"]
        if isinstance(fiscal_year, int):
            _merge_financial_period(
                _financial_period(report_periods, fiscal_year),
                period,
            )
        if monetary_unit is not None:
            units_by_report[report_index] = monetary_unit

    overview_sections = _latest_financial_overviews(document)
    report_count = max(
        len(detail_sections),
        len(overview_sections),
        max(periods_by_report, default=-1) + 1,
    )
    if report_count == 0:
        return []

    scopes = _financial_scopes(overview_sections, report_count=report_count)
    for report_index, overview in enumerate(overview_sections):
        report_periods = periods_by_report.get(report_index)
        if not report_periods:
            continue
        latest_period = report_periods[max(report_periods)]
        _merge_financial_period(latest_period, overview[1])
        if overview[2] is not None:
            units_by_report[report_index] = overview[2]

    _add_employee_history(
        document,
        periods_by_report=periods_by_report,
        scopes=scopes,
    )

    reports: list[JsonObject] = []
    for report_index in range(report_count):
        report_periods = periods_by_report.get(report_index, {})
        reports.append(
            {
                "scope": scopes[report_index],
                "monetary_unit": units_by_report.get(report_index),
                "periods": [
                    report_periods[year]
                    for year in sorted(report_periods, reverse=True)
                ],
            }
        )
    return reports


def _financial_tables(document: HtmlElement) -> list[HtmlElement]:
    return _elements(
        document,
        "//table[not(ancestor::table) and ("
        "starts-with(normalize-space(string(./thead/tr[1]/*[1])), 'Nyckeltal') "
        "or starts-with(normalize-space(string(./thead/tr[1]/*[1])), "
        "'Bokslutsperiod') "
        "or starts-with(normalize-space(string(./thead/tr[1]/*[1])), "
        "'Resultaträkning') "
        "or starts-with(normalize-space(string(./thead/tr[1]/*[1])), "
        "'Balansräkning'))]",
    )


def _financial_header_cells(table: HtmlElement) -> list[HtmlElement]:
    rows = _elements(table, "./thead/tr[1]")
    if not rows:
        rows = _elements(table, "./tbody/tr[1]")
    return _elements(rows[0], "./th|./td") if rows else []


def _financial_label(cell: HtmlElement) -> str | None:
    tooltip_label = _first_text(
        cell,
        ".//*[contains(concat(' ', normalize-space(@class), ' '), "
        "' tooltip-text ')]/preceding-sibling::*[1]",
    )
    if tooltip_label is not None:
        return tooltip_label
    compact_label = _first_text(
        cell,
        ".//*[contains(concat(' ', normalize-space(@class), ' '), ' text-nowrap ')][1]",
    )
    return compact_label or _first_text(cell, ".")


def _financial_category(source_category: str | None) -> str | None:
    if source_category is None:
        return None
    return next(
        (
            category
            for prefix, category in FINANCIAL_TABLE_CATEGORIES.items()
            if source_category.startswith(prefix)
        ),
        None,
    )


def _financial_year(cell: HtmlElement) -> int | None:
    value = _first_text(cell, ".")
    return int(value) if value is not None and re.fullmatch(r"\d{4}", value) else None


def _apply_financial_table(
    table: HtmlElement,
    *,
    category: str,
    fiscal_years: list[int],
    periods: dict[int, JsonObject],
) -> None:
    expected_cell_count = len(fiscal_years) + 1
    for row in _elements(table, "./tbody/tr"):
        cells = _elements(row, "./th|./td")
        if len(cells) != expected_cell_count:
            continue
        label = _financial_label(cells[0])
        if label is None:
            continue
        values = [_first_text(cell, ".") for cell in cells[1:]]
        for fiscal_year, raw_value in zip(fiscal_years, values, strict=True):
            period = _financial_period(periods, fiscal_year)
            if category == "fiscal_period":
                _apply_fiscal_period_value(period, label=label, raw_value=raw_value)
                continue
            metric_key = FINANCIAL_METRIC_KEYS[category].get(label)
            if metric_key is None:
                continue
            metrics = period[category]
            if isinstance(metrics, dict):
                metrics[metric_key] = _financial_number(raw_value)


def _financial_period(periods: dict[int, JsonObject], fiscal_year: int) -> JsonObject:
    return periods.setdefault(
        fiscal_year,
        {
            "fiscal_year": fiscal_year,
            "period_start": None,
            "period_end": None,
            "period_months": None,
            "income_statement": {},
            "balance_sheet": {},
            "key_ratios": {},
            "dividend": None,
            "employee_count": None,
        },
    )


def _apply_fiscal_period_value(
    period: JsonObject,
    *,
    label: str,
    raw_value: str | None,
) -> None:
    if label == "Bokslutsperiod" and raw_value is not None:
        match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})", raw_value)
        if match is not None:
            period["period_start"] = match.group(1)
            period["period_end"] = match.group(2)
    elif label == "Bokslutslängd":
        period["period_months"] = _financial_integer(raw_value)


def _latest_financial_details(
    document: HtmlElement,
) -> list[tuple[str | None, JsonObject]]:
    sections: list[tuple[str | None, JsonObject]] = []
    for heading in _elements(
        document,
        "//h2[starts-with(normalize-space(.), 'Resultat- och balansräkning')]",
    ):
        container = heading.getparent()
        if container is None:
            continue
        container = container.getparent()
        if container is None:
            continue
        heading_text = _first_text(heading, ".")
        monetary_unit = _monetary_unit(heading_text)
        period: JsonObject | None = None
        for card in _elements(
            container,
            ".//*[contains(concat(' ', normalize-space(@class), ' '), "
            "' table-result ')][.//h3]",
        ):
            title = _first_text(card, ".//h3[1]")
            match = re.fullmatch(
                r"(Resultaträkning|Balansräkning|Nyckeltal)\s+(\d{4})",
                title or "",
            )
            if match is None:
                continue
            category = FINANCIAL_TABLE_CATEGORIES[match.group(1)]
            fiscal_year = int(match.group(2))
            if period is None:
                period = _financial_period({}, fiscal_year)
            for row in _elements(
                card,
                ".//*[contains(concat(' ', normalize-space(@class), ' '), "
                "' table-result-content ')]"
                "/div[contains(concat(' ', normalize-space(@class), ' '), "
                "' row ')]",
            ):
                cells = _elements(row, "./div")
                if len(cells) != 2:
                    continue
                label = _first_text(cells[0], ".")
                raw_value = _first_text(cells[1], ".")
                if label == "Periodens längd":
                    period["period_months"] = _financial_integer(raw_value)
                    continue
                if label == "Antal anställda":
                    period["employee_count"] = _financial_integer(raw_value)
                    continue
                metric_key = FINANCIAL_METRIC_KEYS[category].get(label or "")
                metrics = period[category]
                if metric_key is not None and isinstance(metrics, dict):
                    metrics[metric_key] = _financial_number(raw_value)
        if period is not None:
            sections.append((monetary_unit, period))
    return sections


def _latest_financial_overviews(
    document: HtmlElement,
) -> list[tuple[str, JsonObject, str | None]]:
    overviews: list[tuple[str, JsonObject, str | None]] = []
    for heading in _elements(
        document,
        "//h2[starts-with(normalize-space(.), 'Översikt senaste') and "
        "following-sibling::div[contains(concat(' ', normalize-space(@class), "
        "' '), ' block-multi-box-four ')]]",
    ):
        title = _first_text(heading, ".") or ""
        scope = "consolidated" if "koncernbokslut" in title else "company"
        period: JsonObject = {
            "income_statement": {},
            "key_ratios": {},
        }
        monetary_unit: str | None = None
        for card in _elements(
            heading,
            "following-sibling::div[contains(concat(' ', "
            "normalize-space(@class), ' '), ' block-multi-box-four ')][1]"
            "/div[contains(concat(' ', normalize-space(@class), ' '), "
            "' block-multi-box-four-item ')]",
        ):
            label = _financial_label_from_heading(card)
            raw_value = _first_text(
                card,
                ".//div[contains(concat(' ', normalize-space(@class), ' '), "
                "' block-multi-box-four-item__ingress ')][1]",
            )
            monetary_unit = monetary_unit or _value_unit(raw_value)
            value = _financial_number(raw_value)
            income_statement = period["income_statement"]
            key_ratios = period["key_ratios"]
            if label == "Omsättning" and isinstance(income_statement, dict):
                income_statement["revenue"] = value
            elif label == "Årets resultat" and isinstance(income_statement, dict):
                income_statement["net_income"] = value
            elif label == "EBITDA" and isinstance(key_ratios, dict):
                key_ratios["ebitda"] = value
            elif label == "Utdelning":
                period["dividend"] = value
        overviews.append((scope, period, monetary_unit))
    return overviews


def _financial_label_from_heading(element: HtmlElement) -> str | None:
    heading = _elements(element, ".//h3[1]")
    return _financial_label(heading[0]) if heading else None


def _financial_scopes(
    overviews: list[tuple[str, JsonObject, str | None]],
    *,
    report_count: int,
) -> list[str]:
    scopes = [overview[0] for overview in overviews]
    if len(scopes) == report_count:
        return scopes
    if report_count == 1:
        return ["company"]
    fallback = ["consolidated", "company"]
    fallback.extend(f"report_{index}" for index in range(3, report_count + 1))
    return fallback[:report_count]


def _merge_financial_period(target: JsonObject, source: JsonObject) -> None:
    for key in ("income_statement", "balance_sheet", "key_ratios"):
        target_section = target.get(key)
        source_section = source.get(key)
        if isinstance(target_section, dict) and isinstance(source_section, dict):
            target_section.update(source_section)
    for key in (
        "period_start",
        "period_end",
        "period_months",
        "dividend",
        "employee_count",
    ):
        value = source.get(key)
        if value is not None:
            target[key] = value


def _add_employee_history(
    document: HtmlElement,
    *,
    periods_by_report: dict[int, dict[int, JsonObject]],
    scopes: list[str],
) -> None:
    for report_index, scope in enumerate(scopes):
        container_id = (
            "antal-anstallda" if scope == "consolidated" else "antal-anstallda-bolag"
        )
        tables = _elements(document, f"//*[@id='{container_id}']//noscript//table")
        if not tables:
            continue
        cells = _elements(tables[0], "./tbody/tr[1]/td")
        if len(cells) != 2:
            continue
        years = _text_nodes(cells[0])
        employee_counts = _text_nodes(cells[1])
        report_periods = periods_by_report.setdefault(report_index, {})
        for raw_year, raw_count in zip(years, employee_counts, strict=False):
            if not re.fullmatch(r"\d{4}", raw_year):
                continue
            _financial_period(report_periods, int(raw_year))["employee_count"] = (
                _financial_integer(raw_count)
            )


def _text_nodes(element: HtmlElement) -> list[str]:
    values: list[str] = []
    for text_value in element.xpath(".//text()"):
        if not isinstance(text_value, str):
            continue
        normalized = _normalize_text(text_value)
        if normalized is not None:
            values.append(normalized)
    return values


def _monetary_unit(value: str | None) -> str | None:
    if value is None:
        return None
    match = re.search(r"\((M?T?SEK)\)", value)
    return match.group(1) if match is not None else None


def _value_unit(value: str | None) -> str | None:
    if value is None:
        return None
    match = re.search(r"\b(M?T?SEK)\s*$", value)
    return match.group(1) if match is not None else None


def _financial_number(value: str | None) -> int | float | None:
    normalized = _normalize_financial_number(value)
    if normalized is None:
        return None
    try:
        return float(normalized) if "." in normalized else int(normalized)
    except ValueError:
        return None


def _financial_integer(value: str | None) -> int | None:
    number = _financial_number(value)
    if isinstance(number, int):
        return number
    if isinstance(number, float) and number.is_integer():
        return int(number)
    return None


def _normalize_financial_number(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().replace("−", "-").replace("–", "-")
    normalized = re.sub(r"\s+", "", normalized)
    normalized = re.sub(
        r"(?:MSEK|TSEK|SEK|st|%)$",
        "",
        normalized,
        flags=re.IGNORECASE,
    )
    if not normalized or normalized == "-":
        return None
    if "," in normalized:
        normalized = normalized.replace(".", "").replace(",", ".")
    return normalized


def _responsible_people(
    document: HtmlElement,
    *,
    source_url: str,
) -> list[JsonObject]:
    rows = _elements(
        document,
        "//h3[starts-with(normalize-space(.), 'Befattningshavare')]"
        "/following-sibling::div[contains(concat(' ', normalize-space(@class), "
        "' '), ' table-standard ')][1]//tbody/tr",
    )
    people: list[JsonObject] = []
    for row in rows:
        profile_path = _first_text(row, ".//td[1]//a[1]/@href")
        people.append(
            {
                "display_name": _first_text(row, ".//td[1]//a[1]"),
                "role": _first_text(row, "./td[2]"),
                "profile_url": (
                    urljoin(source_url, profile_path)
                    if profile_path is not None
                    else None
                ),
            }
        )
    return people


def _workplaces(
    document: HtmlElement,
    *,
    json_ld_items: list[Mapping[str, object]],
) -> list[JsonObject]:
    workplace_list = next(
        (
            item
            for item in json_ld_items
            if item.get("@type") == "ItemList" and item.get("name") == "Arbetsställen"
        ),
        None,
    )
    if workplace_list is not None:
        workplaces = _json_ld_workplaces(workplace_list)
        if workplaces:
            return workplaces

    return [
        {
            "name": _first_text(
                table, ".//thead//*[contains(normalize-space(), ':')][1]"
            ),
            "identifier": _table_field(table, "Arbetsställenummer"),
            "industry": _industry_value(_table_field(table, "SNI-kod")),
            "address": {
                "street": _table_field(table, "Besöksadress"),
                "postal_code": _table_field(table, "Postnummer"),
                "locality": _table_field(table, "Postort"),
                "county": _table_field(table, "Län"),
            },
            "number_of_employees": _table_field(table, "Antal anställda"),
        }
        for table in _elements(
            document,
            "//table[.//td[normalize-space()='Arbetsställenummer:']]",
        )
    ]


def _json_ld_workplaces(item_list: Mapping[str, object]) -> list[JsonObject]:
    raw_items = item_list.get("itemListElement")
    values = raw_items if isinstance(raw_items, list) else [raw_items]
    workplaces: list[JsonObject] = []
    for value in values:
        if not isinstance(value, dict):
            continue
        address = _mapping_value(value, "address")
        employee_count = _mapping_value(value, "numberOfEmployees")
        workplaces.append(
            {
                "name": _string_value(value, "name"),
                "identifier": _string_value(value, "identifier"),
                "industry": _industry_value(_string_value(value, "description")),
                "address": {
                    "street": _string_value(address, "streetAddress"),
                    "postal_code": _string_value(address, "postalCode"),
                    "locality": _string_value(address, "addressLocality"),
                    "county": _string_value(address, "addressRegion"),
                },
                "number_of_employees": _string_value(employee_count, "name"),
            }
        )
    return workplaces


def _people_at_address(
    document: HtmlElement,
    *,
    source_url: str,
) -> list[JsonObject]:
    rows = _elements(
        document,
        "//*[@id='paAdressen']/following::h3[normalize-space()='Personer'][1]"
        "/following-sibling::div[contains(concat(' ', normalize-space(@class), "
        "' '), ' row ')]",
    )
    people: list[JsonObject] = []
    for row in rows:
        profile_path = _first_text(
            row,
            ".//a[not(contains(@href, '/kop/'))][1]/@href",
        )
        display_name = _first_text(
            row,
            ".//a[not(contains(@href, '/kop/'))][1]",
        )
        if display_name is None:
            continue
        name, age = _person_name_and_age(display_name)
        people.append(
            {
                "name": name,
                "age": age,
                "profile_url": (
                    urljoin(source_url, profile_path)
                    if profile_path is not None
                    else None
                ),
            }
        )
    return people


def _person_name_and_age(display_name: str) -> tuple[str, int | None]:
    match = re.fullmatch(r"(.+?)\s+\((\d+)\s+år\)", display_name)
    if match is None:
        return display_name, None
    return match.group(1), int(match.group(2))


def _industry_value(value: str | None) -> JsonObject | None:
    if value is None:
        return None
    code, separator, description = value.partition(" - ")
    return {
        "code": code or None,
        "description": description if separator else None,
    }


def _table_field(table: HtmlElement, label: str) -> str | None:
    return _first_text(
        table,
        ".//tr[td[1][normalize-space()=$label]]/td[2]",
        label=f"{label}:",
    )


def _mapping_value(
    value: Mapping[str, object] | None,
    key: str,
) -> Mapping[str, object] | None:
    if value is None:
        return None
    nested_value = value.get(key)
    return nested_value if isinstance(nested_value, dict) else None


def _string_value(value: Mapping[str, object] | None, key: str) -> str | None:
    if value is None:
        return None
    raw_value = value.get(key)
    return _normalize_text(raw_value) if isinstance(raw_value, str) else None


def _number_value(
    value: Mapping[str, object] | None,
    key: str,
) -> int | float | None:
    if value is None:
        return None
    raw_value = value.get(key)
    if isinstance(raw_value, bool):
        return None
    if isinstance(raw_value, int | float):
        return raw_value if math.isfinite(raw_value) else None
    if not isinstance(raw_value, str):
        return None
    try:
        parsed_value = float(raw_value)
    except ValueError:
        return None
    return parsed_value if math.isfinite(parsed_value) else None


def _first_text(
    element: HtmlElement,
    xpath: str,
    **variables: str,
) -> str | None:
    results = element.xpath(xpath, **variables)
    if not results:
        return None
    result = results[0]
    if isinstance(result, str):
        return _normalize_text(result)
    if isinstance(result, HtmlElement):
        return _normalize_text(" ".join(result.itertext()))
    return _normalize_text(str(result))


def _all_text(element: HtmlElement, xpath: str) -> list[str]:
    values: list[str] = []
    for result in element.xpath(xpath):
        if isinstance(result, str):
            value = _normalize_text(result)
        elif isinstance(result, HtmlElement):
            value = _normalize_text(" ".join(result.itertext()))
        else:
            value = _normalize_text(str(result))
        if value is not None:
            values.append(value)
    return values


def _elements(element: HtmlElement, xpath: str) -> list[HtmlElement]:
    return [
        result for result in element.xpath(xpath) if isinstance(result, HtmlElement)
    ]


def _normalize_text(value: str) -> str | None:
    normalized = re.sub(r"\s+", " ", value).strip()
    return normalized or None


def _first_responsible_profile_url(report: JsonObject) -> str | None:
    people = report.get("responsible_people")
    if not isinstance(people, list):
        return None
    for person in people:
        if not isinstance(person, dict):
            continue
        profile_url = person.get("profile_url")
        if isinstance(profile_url, str) and profile_url:
            return profile_url
    return None


def _browser_settings(
    process_settings: ProcessSettings,
    browser_id: str,
) -> BrowserSettings:
    browser_settings = next(
        (
            browser
            for browser in process_settings.browsers
            if browser.browser_id == browser_id
        ),
        None,
    )
    if browser_settings is None:
        available = ", ".join(
            browser.browser_id for browser in process_settings.browsers
        )
        raise ValueError(
            f"browser {browser_id!r} is not enabled; available browsers: {available}"
        )
    return browser_settings


def _page_timeout_ms(environment: Mapping[str, str]) -> int:
    raw_timeout = environment.get(
        "RATSIT_PAGE_TIMEOUT_MS",
        str(DEFAULT_PAGE_TIMEOUT_MS),
    ).strip()
    try:
        timeout_ms = int(raw_timeout)
    except ValueError as error:
        raise ValueError("RATSIT_PAGE_TIMEOUT_MS must be an integer") from error
    if timeout_ms < 1:
        raise ValueError("RATSIT_PAGE_TIMEOUT_MS must be positive")
    return timeout_ms


@click.command()
@click.argument("url")
@click.option(
    "--config",
    "config_path",
    type=click.Path(
        path_type=Path,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    envvar="RATSIT_PROCESS_CONFIG",
    required=True,
    help="Protected TOML file containing direct and proxy browser entries.",
)
@click.option(
    "--browser",
    "browser_id",
    default="direct",
    show_default=True,
    help="Enabled browser ID from the process TOML.",
)
@click.option(
    "--follow-people/--no-follow-people",
    default=True,
    show_default=True,
    help="Follow the first responsible-person profile to populate people_at_address.",
)
@click.option(
    "--headless/--headed",
    default=None,
    help="Override process.headless for this extraction.",
)
def main(
    url: str,
    config_path: Path,
    browser_id: str,
    follow_people: bool,
    headless: bool | None,
) -> None:
    """Extract one Ratsit company URL and print normalized JSON."""
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        report = asyncio.run(
            extract_ratsit_url(
                url,
                process_settings=ProcessSettings.from_file(config_path),
                browser_id=browser_id,
                environment=os.environ,
                headless=headless,
                follow_people=follow_people,
            )
        )
    except (OSError, PlaywrightError, RuntimeError, ValueError) as error:
        click.echo(f"Error: {error}", err=True)
        raise SystemExit(1) from error
    click.echo(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
