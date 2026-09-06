# SE company address entity, slice 2a: normalizer v2, the geocode function and cache adoption — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the normalizer's v2 rules, the deterministic geocode function the fold will call (cache lookup in `se_address_geocodes`, the existing OSM matcher for misses, the centroid fallback, cache write-back), and the one-off adoption of the current geocode outcomes onto the new location keys; re-normalize and adopt on prod.

**Architecture:** The normalizer gains four rules and a version bump; every raw row re-normalizes on the next run. `address/geocode.py` wraps the existing DuckDB matcher engine: query documents are built from normalized rows into per-run tables in the OSM workbench, matched against persistent reference documents (built once per OSM extract by a new public wrapper in the shadow driver), and the outcomes are cached in `se_address_geocodes` keyed by a location key (the identity without care-of) so one physical address is matched once whoever receives mail there. The centroid fallback is applied on the way out, never stored. The adoption asset recomputes location keys for the old identities with the same normalizer and copies their current outcomes under the new keys.

**Tech Stack:** Dagster 1.13.9 (`uv run --frozen --no-sync`), Python 3.14, DuckDB (the OSM workbench `data/sweden_address_osm_source.duckdb`), ClickHouse 26.5, pytest (in-memory DuckDB fixtures for the engine, clickhouse-local through docker for the store SQL).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md` (sections 3.7, 4, 6, 9 slice 2, 10). Slice 2 is split: 2a (this plan) is the geocode function, its cache and the normalizer; 2b is the fold, the precedence export and the fold assets.

## Global Constraints

- All work in `corpscout/services/dagster_v3` of the worktree; tests run as `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/<file> -q`; `uv run --frozen --no-sync dg check defs` must pass before every commit that touches `src/`.
- `NORMALIZER_VERSION` becomes `se-address-normalizer-v2`. The 48 existing golden cases keep their expectations byte-identical; 13 cases are added. New rules: a box number may be written with a space (`Box 531 65` → `53165`); a box after a reference or name (`NABO 118849 BOX 843`) is the box with the prefix as care-of when none was delivered; `plan N`, `ii`/`iii`/`iv`, bare four-digit apartment numbers, `n b`/`nb` and `kv` are units; text after a box is dropped with a note.
- Geocode cache key (ruling 2026-09-06, amends spec 3.7): `location_key` = sha256 over the seven location components `country_code, postal_code, city, street_name, box, house_number, unit` joined with `\n` (NULL as `''`); it is what `se_address_geocodes.address_id` holds for rows written by this entity. `address_key` (with care-of) stays the published identity. Cache rows are the matcher's raw outcomes (`matched_*`, `unmatched`, `ambiguous`, `postal_box`); the centroid fallback is applied on read and never stored.
- Versions: the matcher policy is `SWEDEN_ADDRESS_RESOLUTION_POLICY.version` (`se-address-resolution-policy-v7`); the reference version is `geocode_demand.fresh_reference_md5(connection)` read from the workbench (`first(source_md5 order by source_record_id)` over `sweden_address_osm.address_points`). Cache lookups and writes use exactly these two.
- The engine is called through `address_resolution.resolution.replace_address_resolution_candidates` and `replace_address_resolution_results` with `SWEDEN_ADDRESS_RESOLUTION_POLICY`, on query documents built by `address_resolution.search_documents.replace_address_search_documents` and street variants by `replace_address_street_variants` with the Sweden expansion tables from `sweden_company/address_resolution_policy.py`, exactly as `address_resolution_shadow.replace_sweden_address_resolution_shadow` does; reference documents come from the shadow driver's builders through a new public wrapper. Nothing in the engine, the policy or the OSM assets changes.
- Box addresses carry `address_kind = 'postal_box'` in their query document (the engine returns `postal_box`); everything else `physical`.
- Fallback on read: statuses `unmatched`, `ambiguous`, `postal_box` get the postcode centroid when `se_postcode_centroids.point_count > 0 AND spread_meters <= 3000.0`, else the city centroid when `se_city_centroids.point_count > 0`, labelled `match_status 'matched_area'`, `geocode_provider 'centroid_fallback'`, `geocode_precision 'postcode'` or `'city'`, `coordinate_method 'centroid_median'`; keys through `centroid_keys.postcode_key_sql`/`city_key_sql`.
- The workbench is opened through the `sweden_address_osm_duckdb` resource; assets that open it carry `pool = sweden_address_osm.tables.DUCKDB_POOL`. Per-run tables in the workbench are named with the run id and dropped in a `finally`.
- Cache writes go to `corpscout.se_address_geocodes` over `geocode_store.STORE_COLUMNS` with `address_identity_run_id = 'address-entity'` for matched rows and `'adopted:<old address_id>'` for adopted rows, `normalized_match_key` = the normalized display line, `geocode_run_id` = the Dagster run id, `matched_at` = the run's stamp.
- No `from __future__ import annotations` in any module that defines a `@dg.asset`.
- Commit by explicit path after every task; never `git add -A`. Trailers, contiguous at the end of every commit message: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` then `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`.

---

## File structure

- Modify: `src/dagster_v3/defs/se_company/address/normalize_se.py` — v2 rules, `LOCATION_FIELDS`, `location_key`.
- Modify: `tests/fixtures/se_addresses/golden.jsonl` (+13 lines), `tests/test_se_company_address_normalize_se.py` (version, location key).
- Modify: `src/dagster_v3/defs/sweden_company/address_resolution_shadow.py` — public `replace_reference_documents` and `reference_documents_md5` (a manifest table), composed from the existing private builders.
- Create: `src/dagster_v3/defs/se_company/address/geocode.py` — `GeocodeOutcome`, `geocode_addresses`, the cache read/write SQL, the query-document projection, the fallback SQL.
- Create: `src/dagster_v3/defs/se_company/address/adoption.py` — the adoption asset `se_address_geocodes_adopt_keys`.
- Tests: `tests/test_se_company_address_geocode.py`, `tests/test_se_company_address_geocode_clickhouse_local.py`, `tests/test_se_company_address_adoption.py`, `tests/test_sweden_address_reference_documents.py`.

---

### Task 1: Normalizer v2 and the location key

**Files:**
- Modify: `src/dagster_v3/defs/se_company/address/normalize_se.py`
- Modify: `tests/fixtures/se_addresses/golden.jsonl`, `tests/test_se_company_address_normalize_se.py`
- Modify: `src/dagster_v3/defs/se_company/address/docs/address-design.md` (the parse rules paragraph)

**Interfaces:**
- Produces: `NORMALIZER_VERSION == "se-address-normalizer-v2"`; `LOCATION_FIELDS = ("country_code", "postal_code", "city", "street_name", "box", "house_number", "unit")`; `location_components(n) -> tuple[str, ...]`; `location_key(n) -> str` (64 hex). `identity_components`/`address_key` unchanged.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_se_company_address_normalize_se.py`, and change the version assertion in `test_version_constant` to `"se-address-normalizer-v2"`)

```python
from dagster_v3.defs.se_company.address.normalize_se import LOCATION_FIELDS, location_components, location_key


def test_location_key_ignores_care_of_and_is_the_seven_components() -> None:
    assert LOCATION_FIELDS == ("country_code", "postal_code", "city", "street_name", "box", "house_number", "unit")
    with_care_of = normalize_se_address(RawAddress(care_of="c/o Anna Svensson", street_address="Kungsgatan 4 A, 3 tr",
                                                   postal_code="11143", post_town="Stockholm"))
    without = normalize_se_address(RawAddress(street_address="Kungsgatan 4 A, 3 tr", postal_code="11143", post_town="Stockholm"))
    assert location_components(with_care_of) == ("SE", "11143", "stockholm", "kungsgatan", "", "4A", "3 tr")
    assert location_key(with_care_of) == location_key(without)
    assert address_key(with_care_of) != address_key(without)
    assert len(location_key(with_care_of)) == 64


def test_v2_rules_box_with_space_box_after_reference_and_new_units() -> None:
    spaced = normalize_se_address(RawAddress(raw_address="Box 531 65$$GÖTEBORG$40015$SE-LAND"))
    assert (spaced.box, spaced.parse_status) == ("53165", "ok")
    referenced = normalize_se_address(RawAddress(street_address="NABO 118849  BOX 843", postal_code="85123", post_town="SUNDSVALL"))
    assert (referenced.care_of, referenced.box, referenced.street_name) == ("nabo 118849", "843", None)
    assert "box after 'nabo 118849'" in referenced.parse_notes
    glued = normalize_se_address(RawAddress(street_address="C/O NABO 233769  BOX 843 851 23 SUND NYÄNGSVÄGEN 1", postal_code="85123", post_town="SUNDSVALL"))
    assert (glued.care_of, glued.box) == ("nabo 233769", "843")
    assert "dropped trailing text '851 23 sund nyängsvägen 1'" in glued.parse_notes
    for line, unit in (("Varengatan 35, 1302", "1302"), ("Storgatan 12 plan 5", "plan 5"), ("Kungsgatan 8 II", "ii"),
                       ("Splintvägen 14 n b", "nb"), ("Prostgatan 10, kv", "kv")):
        n = normalize_se_address(RawAddress(street_address=line, postal_code="11122", post_town="Stockholm"))
        assert n.unit == unit, line
        assert n.house_number in ("35", "12", "8", "14", "10"), line
```

Corpus: append these 13 lines to `tests/fixtures/se_addresses/golden.jsonl` (they were produced by the v2 code and reviewed; the corpus test then counts 61 cases):

```jsonl
{"source": "scb", "raw": {"street_address": "NABO 118849  BOX 843", "postal_code": "85123", "post_town": "SUNDSVALL"}, "expected": {"care_of": "nabo 118849", "box": "843", "street_name": null, "house_number": null, "unit": null, "postal_code": "85123", "city": "sundsvall", "country_code": "SE", "normalized_address": "c/o Nabo 118849, Box 843, 851 23 Sundsvall", "parse_status": "ok", "parse_notes": "box after 'nabo 118849'"}}
{"source": "scb", "raw": {"street_address": "C/O NABO 233769  BOX 843 851 23 SUND NYÄNGSVÄGEN 1", "postal_code": "85123", "post_town": "SUNDSVALL"}, "expected": {"care_of": "nabo 233769", "box": "843", "street_name": null, "house_number": null, "unit": null, "postal_code": "85123", "city": "sundsvall", "country_code": "SE", "normalized_address": "c/o Nabo 233769, Box 843, 851 23 Sundsvall", "parse_status": "ok", "parse_notes": "care-of split before 'nabo 233769 box 843 851 23 sund nyängsvägen 1'; box after 'nabo 233769'; dropped trailing text '851 23 sund nyängsvägen 1'"}}
{"source": "bolagsverket", "raw": {"raw_address": "Box 531 65$$GÖTEBORG$40015$SE-LAND"}, "expected": {"care_of": null, "box": "53165", "street_name": null, "house_number": null, "unit": null, "postal_code": "40015", "city": "göteborg", "country_code": "SE", "normalized_address": "Box 53165, 400 15 Göteborg", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Box 12 309$$STOCKHOLM$10228$SE-LAND"}, "expected": {"care_of": null, "box": "12309", "street_name": null, "house_number": null, "unit": null, "postal_code": "10228", "city": "stockholm", "country_code": "SE", "normalized_address": "Box 12309, 102 28 Stockholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Box 843,  SUNDSVALL$C/o Nabo 6547 Brf Roslagsbanan 1$SUNDSVALL$85123$SE-LAND"}, "expected": {"care_of": "nabo 6547 brf roslagsbanan 1", "box": "843", "street_name": null, "house_number": null, "unit": null, "postal_code": "85123", "city": "sundsvall", "country_code": "SE", "normalized_address": "c/o Nabo 6547 Brf Roslagsbanan 1, Box 843, 851 23 Sundsvall", "parse_status": "ok", "parse_notes": "dropped trailing text 'sundsvall'"}}
{"source": "bolagsverket", "raw": {"raw_address": "Splintvägen 14 n b$$ÖSTERSUND$83172$SE-LAND"}, "expected": {"care_of": null, "box": null, "street_name": "splintvägen", "house_number": "14", "unit": "nb", "postal_code": "83172", "city": "östersund", "country_code": "SE", "normalized_address": "Splintvägen 14 nb, 831 72 Östersund", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Varengatan 35, 1302$c/o Tesfalem Solomon$KISTA$16437$SE-LAND"}, "expected": {"care_of": "tesfalem solomon", "box": null, "street_name": "varengatan", "house_number": "35", "unit": "1302", "postal_code": "16437", "city": "kista", "country_code": "SE", "normalized_address": "c/o Tesfalem Solomon, Varengatan 35 1302, 164 37 Kista", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Ormångsgatan 63A 1303$c/o Klym Ruslan$HÄSSELBY$16556$SE-LAND"}, "expected": {"care_of": "klym ruslan", "box": null, "street_name": "ormångsgatan", "house_number": "63A", "unit": "1303", "postal_code": "16556", "city": "hässelby", "country_code": "SE", "normalized_address": "c/o Klym Ruslan, Ormångsgatan 63A 1303, 165 56 Hässelby", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Prostgatan 10, kv$$LIDKÖPING$53138$SE-LAND"}, "expected": {"care_of": null, "box": null, "street_name": "prostgatan", "house_number": "10", "unit": "kv", "postal_code": "53138", "city": "lidköping", "country_code": "SE", "normalized_address": "Prostgatan 10 kv, 531 38 Lidköping", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Storgatan 12 plan 5$$STOCKHOLM$11122$SE-LAND"}, "expected": {"care_of": null, "box": null, "street_name": "storgatan", "house_number": "12", "unit": "plan 5", "postal_code": "11122", "city": "stockholm", "country_code": "SE", "normalized_address": "Storgatan 12 plan 5, 111 22 Stockholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Kungsgatan 8 II$$STOCKHOLM$11143$SE-LAND"}, "expected": {"care_of": null, "box": null, "street_name": "kungsgatan", "house_number": "8", "unit": "ii", "postal_code": "11143", "city": "stockholm", "country_code": "SE", "normalized_address": "Kungsgatan 8 ii, 111 43 Stockholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Box 167, Tunalundsvägen 1 A$$Hallstahammar$73424$SE-LAND"}, "expected": {"care_of": null, "box": "167", "street_name": null, "house_number": null, "unit": null, "postal_code": "73424", "city": "hallstahammar", "country_code": "SE", "normalized_address": "Box 167, 734 24 Hallstahammar", "parse_status": "ok", "parse_notes": "dropped trailing text 'tunalundsvägen 1 a'"}}
{"source": "ratsit", "raw": {"street_address": "c/o Bolagspartner avveckling Box 1067", "postal_code": "22104", "post_town": "Lund"}, "expected": {"care_of": "bolagspartner avveckling", "box": "1067", "street_name": null, "house_number": null, "unit": null, "postal_code": "22104", "city": "lund", "country_code": "SE", "normalized_address": "c/o Bolagspartner Avveckling, Box 1067, 221 04 Lund", "parse_status": "ok", "parse_notes": "care-of split before 'box 1067'"}}
```

- [ ] **Step 2: Run to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_normalize_se.py -q`
Expected: the new tests and the 13 new corpus cases FAIL (ImportError for `LOCATION_FIELDS`; the corpus cases on `box`/`unit`/`care_of`); the 48 old cases still pass.

- [ ] **Step 3: Apply the v2 patch to `normalize_se.py`**

Apply exactly this unified diff (it was validated against the 48-case corpus with zero changes and against the 13 new cases); then append the location-key helpers below it:

```diff
--- /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/normalize_se.py	2026-09-06 13:08:55
+++ normalize_se_v2.py	2026-09-06 18:18:16
@@ -13,20 +13,30 @@
 import unicodedata
 from dataclasses import dataclass
 
-NORMALIZER_VERSION = "se-address-normalizer-v1"
+NORMALIZER_VERSION = "se-address-normalizer-v2"
 
 _UNKNOWN_TOWNS = {"okänd", "okand", "adress saknas"}
 _FOREIGN_TOWNS = {"utlandet"}
 _UNKNOWN_STREETS = {"okänd adress", "adress okänd", "okand adress", "adress saknas"}
 _INVALID_POSTCODES = {"00000", "99999"}
 _CARE_OF_PREFIX = re.compile(r"^(?:c/o|c\.o\.|co|att|attn|att:)\s+", re.IGNORECASE)
-_BOX = re.compile(r"^(?:box|postbox|p\.?\s?o\.?\s?box)\s+(?P<box>[0-9]+[a-zåäö]?)$", re.IGNORECASE)
+_BOX = re.compile(
+    r"^(?:box|postbox|p\.?\s?o\.?\s?box)\s+(?P<box>[0-9]+(?:\s[0-9]{2,3}(?![\s,]*[0-9]))?[a-zåäö]?)(?:[\s,]+(?P<rest>\S.*))?$",
+    re.IGNORECASE,
+)
+# A box after a reference or name ("nabo 118849 box 843", "c/o firm box 12"): the box is the
+# address, the prefix is the care-of when none was delivered.
+_BOX_AFTER_PREFIX = re.compile(
+    r"^(?P<prefix>.+?)[\s,]+(?:box|postbox)\s+(?P<box>[0-9]+(?:\s[0-9]{2,3}(?![\s,]*[0-9]))?[a-zåäö]?)(?:[\s,]+(?P<rest>\S.*))?$",
+    re.IGNORECASE,
+)
 _NUMBER = re.compile(
     r"^(?P<name>.*?\S)\s+(?P<number>[0-9]+(?:\s?-\s?[0-9]+)?(?:\s?[a-zåäö])?)(?:[\s,.]+(?P<rest>\S.*))?$",
     re.IGNORECASE,
 )
 _UNIT = re.compile(
-    r"^(?:lgh\s*[0-9]+|[0-9]+\s*tr\.?|tr\s*[0-9]+|bv|nb|t[0-9]+|[0-9]+\s*(?:vån|van)\.?|vån\s*[0-9]+|uppg\.?\s*[a-z0-9]+|ing\.?\s*[a-z0-9]+)$",
+    r"^(?:lgh\s*[0-9]+|[0-9]+\s*tr\.?|tr\s*[0-9]+|bv|nb|n\s?b|kv|t[0-9]+|[0-9]+\s*(?:vån|van)\.?|vån\s*[0-9]+"
+    r"|uppg\.?\s*[a-z0-9]+|ing\.?\s*[a-z0-9]+|plan\s*[0-9]+|ii|iii|iv|[0-9]{4})$",
     re.IGNORECASE,
 )
 _GLUED_NUMBER = re.compile(r"^(?P<word>[a-zåäöé]+)(?P<number>[0-9]+[a-zåäö]?)$")
@@ -107,9 +117,18 @@
 
 def _split_street(line: str, notes: list[str]) -> tuple[str | None, str | None, str | None, str | None]:
     """-> (box, street_name, house_number, unit) from a folded street line."""
+    line = re.sub(r"\bn\s+b$", "nb", line)  # "nedre botten" written as two letters
     box = _BOX.match(line)
     if box:
-        return box.group("box"), None, None, None
+        if box.group("rest"):
+            notes.append(f"dropped trailing text '{box.group('rest').strip(' .,')}'")
+        return re.sub(r"\s+", "", box.group("box")), None, None, None
+    prefixed = _BOX_AFTER_PREFIX.match(line)
+    if prefixed:
+        notes.append(f"box after '{prefixed.group('prefix')}'")
+        if prefixed.group("rest"):
+            notes.append(f"dropped trailing text '{prefixed.group('rest').strip(' .,')}'")
+        return re.sub(r"\s+", "", prefixed.group("box")), None, None, None
     line = re.sub(r"\s*,\s*", " ", line)
     m = _NUMBER.match(line)
     if not m:
@@ -182,6 +201,10 @@
         care_of_display = _display(care_of or "")
         street_display_source = ""
     box, street_name, house_number, unit = _split_street(street_line, notes) if street_line else (None, None, None, None)
+    prefix_note = next((n for n in notes if n.startswith("box after '")), None)
+    if prefix_note and not care_of:
+        care_of = _CARE_OF_PREFIX.sub("", prefix_note[len("box after '"):-1]).strip()
+        care_of_display = _display(care_of)
 
     if code and (len(code) != 5 or code in _INVALID_POSTCODES):
         notes.append(f"postcode '{raw.postal_code}' is not a valid five-digit code")
```

Append after `address_key`:

```python
LOCATION_FIELDS: tuple[str, ...] = ("country_code", "postal_code", "city", "street_name", "box", "house_number", "unit")


def location_components(normalized: NormalizedAddress) -> tuple[str, ...]:
    """The identity without care-of: what the geocoder sees. One physical address is
    matched once whoever receives mail there (spec 3.7 as amended for slice 2a)."""
    return tuple(getattr(normalized, field_name) or "" for field_name in LOCATION_FIELDS)


def location_key(normalized: NormalizedAddress) -> str:
    return hashlib.sha256("\n".join(location_components(normalized)).encode("utf-8")).hexdigest()
```

One more rule the diff does not carry, add it by hand in `_split_care_of_street`: before tokenizing, if `_BOX_AFTER_PREFIX.match(stripped) or _BOX.match(stripped)` then `return None, stripped` (no "care-of split" note; the box rules take over in `_split_street`). Add a unit test asserting `normalize_se_address(RawAddress(street_address="c/o Bolagspartner avveckling Box 1067", postal_code="22104", post_town="Lund"))` has `care_of == "bolagspartner avveckling"`, `box == "1067"` and no `care-of split` note.

Docs: in `address/docs/address-design.md`'s parse rules paragraph, add one sentence per new rule and the location-key definition.

- [ ] **Step 4: Run to verify they pass**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_normalize_se.py tests/test_se_company_address_normalize.py tests/test_se_company_address_extractors_sql.py -q`
Expected: all PASS (the corpus test now reports 61 cases). `uv run --frozen --no-sync dg check defs`: green.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/normalize_se.py \
  corpscout/services/dagster_v3/tests/fixtures/se_addresses/golden.jsonl \
  corpscout/services/dagster_v3/tests/test_se_company_address_normalize_se.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md
git commit -m "feat(dagster): Swedish address normalizer v2 and the location key"
```

---

### Task 2: Persistent reference documents in the workbench

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/address_resolution_shadow.py`
- Test: `tests/test_sweden_address_reference_documents.py`

**Interfaces:**
- Consumes: the module's private `_replace_building_reference_documents(connection)`, `_replace_street_reference_documents(connection)` and the union step that fills `QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE` inside `replace_sweden_address_resolution_shadow` (read that function to find the exact union statement and reuse it); `geocode_demand.fresh_reference_md5(connection)`.
- Produces: `REFERENCE_MANIFEST_TABLE = "se_address_resolution_reference_manifest"` in the enrichment schema (`QUALIFIED_REFERENCE_MANIFEST_TABLE`), columns `(reference_md5 varchar, policy_version varchar, built_at timestamp)`; `replace_reference_documents(connection, *, log=None) -> str` (rebuilds building + street docs into `QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE`, writes one manifest row, returns the md5); `reference_documents_md5(connection) -> str` (`''` when no manifest); `ensure_reference_documents(connection, *, log=None) -> str` (rebuild when the manifest's md5 differs from `fresh_reference_md5`, else no-op; returns the current md5).

- [ ] **Step 1: Write the failing test**

Build an in-memory DuckDB with the two OSM tables the builders read (`sweden_address_osm.address_points`, `sweden_address_osm.street_segments`): copy the fixture-building helper the existing shadow tests use (grep `tests/` for `address_points` and `street_segments` inserts, e.g. in `tests/test_sweden_company_address_geocoding.py`, and reuse its DDL by import or by copying the `create table` statements), insert three address points on one street in one postcode with a `source_md5 'md5-a'`, then:

```python
def test_reference_documents_are_built_once_per_extract(connection) -> None:
    assert reference_documents_md5(connection) == ""
    first = ensure_reference_documents(connection)
    assert first == "md5-a"
    rows = connection.execute(f"select count(*) from {QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE}").fetchone()[0]
    assert rows > 0
    built_at = connection.execute(f"select built_at from {QUALIFIED_REFERENCE_MANIFEST_TABLE}").fetchone()[0]
    assert ensure_reference_documents(connection) == "md5-a"
    assert connection.execute(f"select built_at from {QUALIFIED_REFERENCE_MANIFEST_TABLE}").fetchone()[0] == built_at  # no rebuild
    connection.execute("update sweden_address_osm.address_points set source_md5 = 'md5-b'")
    assert ensure_reference_documents(connection) == "md5-b"
    assert connection.execute(f"select count(*) from {QUALIFIED_REFERENCE_MANIFEST_TABLE}").fetchone()[0] == 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_sweden_address_reference_documents.py -q`
Expected: FAIL with `ImportError` for the new names.

- [ ] **Step 3: Implement**

In `address_resolution_shadow.py`, add the manifest constants next to the other table names, then:

```python
def replace_reference_documents(connection: Any, *, log: Callable[..., object] | None = None) -> str:
    """Build the building and street reference documents from the current OSM workbench
    tables and record the extract they came from. The shadow evaluation and the address
    entity's geocode function both read the result."""
    reference_md5 = geocode_demand.fresh_reference_md5(connection)
    _replace_building_reference_documents(connection)
    _replace_street_reference_documents(connection)
    <the existing union statement that creates QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE, moved into this function verbatim>
    connection.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
    connection.execute(
        f"create or replace table {QUALIFIED_REFERENCE_MANIFEST_TABLE} as "
        "select ?::varchar as reference_md5, ?::varchar as policy_version, now()::timestamp as built_at",
        [reference_md5, SWEDEN_ADDRESS_RESOLUTION_POLICY.version],
    )
    if log is not None:
        log("reference documents rebuilt for extract %s", reference_md5)
    return reference_md5


def reference_documents_md5(connection: Any) -> str:
    exists = connection.execute(
        "select count(*) from information_schema.tables where table_schema = ? and table_name = ?",
        [ENRICHMENT_SCHEMA, REFERENCE_MANIFEST_TABLE],
    ).fetchone()[0]
    if not exists:
        return ""
    row = connection.execute(f"select reference_md5 from {QUALIFIED_REFERENCE_MANIFEST_TABLE}").fetchone()
    return row[0] if row else ""


def ensure_reference_documents(connection: Any, *, log: Callable[..., object] | None = None) -> str:
    current = geocode_demand.fresh_reference_md5(connection)
    if reference_documents_md5(connection) == current:
        return current
    return replace_reference_documents(connection, log=log)
```

Replace the three builder calls plus the union inside `replace_sweden_address_resolution_shadow` with one call to `replace_reference_documents(connection, log=log)` so there is one builder; keep every other statement of that function unchanged (its tests must stay green: `tests/test_sweden_company_address_geocoding.py` and any test naming `replace_sweden_address_resolution_shadow`).

- [ ] **Step 4: Run to verify it passes**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_sweden_address_reference_documents.py tests/test_sweden_company_address_geocoding.py tests/test_address_resolution.py -q`
Expected: all PASS. `uv run --frozen --no-sync dg check defs`: green.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_resolution_shadow.py \
  corpscout/services/dagster_v3/tests/test_sweden_address_reference_documents.py
git commit -m "refactor(dagster): reference documents built once per OSM extract"
```

---

### Task 3: The geocode function

**Files:**
- Create: `src/dagster_v3/defs/se_company/address/geocode.py`
- Test: `tests/test_se_company_address_geocode.py` (engine on an in-memory DuckDB + a fake ClickHouse client), `tests/test_se_company_address_geocode_clickhouse_local.py` (the cache read/write SQL and the fallback SQL on clickhouse-local, docker)

**Interfaces:**
- Consumes: Task 1's `NormalizedAddress`, `location_key`; Task 2's `ensure_reference_documents`, `QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE`; `address_resolution.search_documents.replace_address_search_documents(connection, *, table_name, source_sql)` and `replace_address_street_variants(...)` (call it exactly as `address_resolution_shadow.replace_sweden_address_resolution_shadow` does, with the Sweden expansion tables); `address_resolution.resolution.replace_address_resolution_candidates/results`; `SWEDEN_ADDRESS_RESOLUTION_POLICY`; `geocode_store.build_current_geocodes_sql(columns=..., address_filter_sql=...)`, `geocode_store.STORE_COLUMNS`, `geocode_store.GEOCODED_STATUSES`; `geocode_serving_overlay.FALLBACK_ELIGIBLE_STATUSES`, `POSTCODE_SPREAD_MAX_METERS`; `centroid_keys.postcode_key_sql`, `city_key_sql`; `geocode_demand.fresh_reference_md5`.
- Produces:

```python
@dataclass(frozen=True, slots=True)
class GeocodeOutcome:
    location_key: str
    match_status: str          # the row's geocode_status
    match_method: str
    match_confidence: float | None
    latitude: float | None
    longitude: float | None
    geocode_provider: str
    geocode_precision: str
    coordinate_method: str
    coordinate_locality: str
    coordinate_supporting_point_count: int
    coordinate_spread_meters: float | None
    policy_version: str
    reference_md5: str
    from_cache: bool

def geocode_addresses(
    addresses: Mapping[str, NormalizedAddress],   # location_key -> a representative normalized address
    *, clickhouse: Any, duckdb: Any, run_id: str, matched_at: datetime, log=None,
) -> dict[str, GeocodeOutcome]
```

plus `cache_lookup_sql(keys: Sequence[str]) -> str` (a `build_current_geocodes_sql` over the eight columns the outcome needs, `address_filter_sql="address_id IN %(keys)s"`), `cache_insert_sql() -> str` (`INSERT INTO corpscout.se_address_geocodes (STORE_COLUMNS) VALUES`), `fallback_sql() -> str` (a `VALUES`-driven join: binds `%(rows)s` as an array of `(key, postal_code, city)` tuples and returns `key, tier, latitude, longitude, locality, point_count, spread_meters` per the Global Constraints' rule), `query_documents_sql(table) -> str` (the projection of the per-run input table into the engine's input columns), `CACHE_LOOKUP_CHUNK = 5_000`, `ADDRESS_ENTITY_RUN_ID = "address-entity"`.

The function, in order: (1) `reference_md5 = ensure_reference_documents(duckdb)`; policy = `SWEDEN_ADDRESS_RESOLUTION_POLICY.version`; (2) cache lookup in chunks of `CACHE_LOOKUP_CHUNK` keys, keeping rows whose `policy_version`/`reference_md5` equal the current pair (rows with other versions are misses; the adopted rows count as hits whatever their versions, mirroring `is_adopted` in the store's rank); (3) misses: create a per-run input table `sweden_company_enrichment._address_fold_input_<run_id hex>` with the engine's input columns from the normalized addresses (`document_id` = location key, `raw_address` = the display line, `search_text` = the display line without the care-of part, `street_name`, `house_number`, `unit`, `postal_code`, `locality` = city, `address_kind` = `'postal_box'` when `box` else `'physical'`, `country_code 'SE'`, the rest NULL/0/''), then `replace_address_search_documents` into `_address_fold_query_<run>`, `replace_address_street_variants` into `_address_fold_variants_<run>`, `replace_address_resolution_candidates` into `_address_fold_candidates_<run>` against `QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE`, `replace_address_resolution_results` into `_address_fold_results_<run>`; read the results (`query_document_id, resolution_status, geocode_precision, match_confidence, match_strategy, latitude, longitude, coordinate_spread_meters, supporting_record_count, matched_locality, candidate_record_ids, candidate_record_urls, candidate_record_count`); drop the four per-run tables in a `finally`; (4) insert one store row per miss (`address_id` = location key, `match_status` = `resolution_status`, `match_method` = `match_strategy`, `geocode_provider 'osm'` when a coordinate exists else `''`, `coordinate_method 'resolver'` when a coordinate exists else NULL, `address_identity_run_id 'address-entity'`, `normalized_match_key` = the display line, `geocode_run_id = run_id`, `matched_at`, the source_* columns NULL); (5) apply the fallback to every outcome (cache hits included) whose status is in `FALLBACK_ELIGIBLE_STATUSES` through one `fallback_sql()` call; (6) return the outcomes.

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_address_geocode.py`: build an in-memory DuckDB the way `tests/test_address_resolution.py` does (its input-table helper `replace_address_search_document_input_table` plus inserts) holding a reference of two building points (`Storgatan 5, 11122 Stockholm` at (59.33, 18.06) and `Storgatan 7` at (59.331, 18.061)) materialized into `QUALIFIED_SHADOW_REFERENCE_DOCUMENTS_TABLE` (call the same `replace_address_search_documents` the shadow uses, then write a manifest row with `reference_md5 'ref-1'` so `ensure_reference_documents` is a no-op; give the OSM `address_points` table one row with `source_md5 'ref-1'` so `fresh_reference_md5` agrees) and a `FakeClient` whose `execute` answers: the cache lookup (rows scripted per key), the fallback (rows scripted per key), and records inserts. Tests:

```python
def test_a_cached_key_is_not_matched_again(...):  # cache returns a matched_exact row for key A -> outcome from_cache True, no DuckDB query tables created, no insert
def test_a_miss_is_matched_and_cached(...):       # key B "Storgatan 5" -> resolution matched_exact with the reference coordinate; one insert row with address_id == B, policy 'se-address-resolution-policy-v7', reference 'ref-1', address_identity_run_id 'address-entity'
def test_a_box_is_postal_box_and_gets_the_town_centroid(...):  # key C box 5305 -> engine says postal_box, fallback scripted with a city centroid -> outcome status 'matched_area', provider 'centroid_fallback', precision 'city'; the cached row still says 'postal_box'
def test_an_unmatched_street_gets_the_postcode_centroid_when_tight(...):  # postcode tier, spread 1200 -> precision 'postcode'
def test_a_stale_cache_row_is_a_miss(...):        # cache row with policy v6 for key B -> matched again and re-inserted under v7
def test_sql_texts(...):                          # cache_lookup_sql binds %(keys)s and uses build_current_geocodes_sql; cache_insert_sql lists STORE_COLUMNS; fallback_sql uses postcode_key_sql/city_key_sql and the 3000.0 cap; query_documents_sql yields the 18 input columns in order
def test_per_run_tables_are_dropped_even_on_failure(...)
```

`tests/test_se_company_address_geocode_clickhouse_local.py` (integration, docker): load migrations `000317` (the store), `000323`/`000324` (centroids) plus whatever those migrations need; insert two store rows for one key (v6 and v7) and one centroid row each; run `cache_lookup_sql` bound with the key and assert the v7 row comes back through the two-stage rank; run `cache_insert_sql` with one `GeocodeOutcome`-shaped tuple and read it back; run `fallback_sql` with two rows (one postcode hit, one city-only) and assert the tiers.

- [ ] **Step 2: Run to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_geocode.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.address.geocode'`.

- [ ] **Step 3: Implement `geocode.py`** per the Interfaces block. Keep it a pure orchestration module: no Dagster imports; the two connections are parameters. Log one line per phase (`cache hits / misses / matched / fallback`).

- [ ] **Step 4: Run to verify they pass**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_geocode.py tests/test_address_resolution.py -q` and, once, `... pytest tests/test_se_company_address_geocode_clickhouse_local.py -q -m integration`.
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/geocode.py \
  corpscout/services/dagster_v3/tests/test_se_company_address_geocode.py \
  corpscout/services/dagster_v3/tests/test_se_company_address_geocode_clickhouse_local.py
git commit -m "feat(dagster): address geocode function on the OSM workbench with the store as cache"
```

---

### Task 4: The adoption asset

**Files:**
- Create: `src/dagster_v3/defs/se_company/address/adoption.py`
- Test: `tests/test_se_company_address_adoption.py`

**Interfaces:**
- Consumes: Task 1's `normalize_se_address`, `location_key`, `RawAddress`; `geocode_store.build_current_geocodes_sql`, `STORE_COLUMNS`, `GEOCODED_STATUSES`; `geocode_demand.QUERY_BATCH_SIZE` (100,000) and its keyset paging idea.
- Produces: asset `se_address_geocodes_adopt_keys` (group `se_company_address`, kinds clickhouse/python, config `AdoptKeysConfig(execute: bool = False, page_size: int = 100_000)`), `identities_page_sql()` (from `corpscout.se_addresses_current`: `address_id, street_address, postal_code, post_town, country_code`, keyset `address_id > %(after)s ORDER BY address_id LIMIT %(page_size)s`), `current_outcomes_sql()` (`build_current_geocodes_sql(columns=STORE_COLUMNS, address_filter_sql="address_id IN %(ids)s")`), `adopted_row(outcome_row, *, new_key, old_id, imported_at) -> tuple` (the STORE_COLUMNS tuple with `address_id = new_key`, `address_identity_run_id = f"adopted:{old_id}"`, `geocode_run_id` the run id, `matched_at` = the original `matched_at`, everything else copied), `AdoptCounts(identities, normalized_ok, keys, adopted, skipped_not_geocoded, skipped_no_address)`.

Rules: for each identity page, run the v2 normalizer on `RawAddress(street_address=..., postal_code=..., post_town=..., country_code=...)`; skip `no_address`/`foreign`/`partial`; compute the location key; several identities may collapse onto one key (different care-of or spelling): keep the first identity per key in `address_id` order and count the rest as `collapsed`; read the identities' current outcomes (both stages of the rank, adopted rows included), keep those with `match_status IN GEOCODED_STATUSES`, and insert the adopted rows in execute mode (preview counts only). A key that already has a store row for the current policy+reference pair is skipped (`existing_keys_sql()` over the store for the page's keys).

- [ ] **Step 1: Write the failing test** with a `FakeClient` scripting two identity pages, outcomes and existing keys; assert the adopted tuples (column by column), the collapse rule, the skip rules, and `execute=False` inserting nothing.
- [ ] **Step 2: Run to verify it fails.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the test and `dg check defs`; `dg list defs | grep se_address_geocodes_adopt_keys` lists the asset (pool: none; it only talks to ClickHouse).
- [ ] **Step 5: Commit** `feat(dagster): adopt current geocode outcomes onto address location keys`.

---

### Task 5: Prod run (controller)

1. [ ] Whole-branch review; the owner merges; hot-sync the dagster host from the deploy worktree.
2. [ ] `se_company_address_normalize` (`changed_only: true`, page 20,000): the version bump selects every raw row; expect 4,757,823 rows re-normalized on `se-address-normalizer-v2`; readout `parse_status` per source again and the count of `box after` notes (expect about 2,100).
3. [ ] `se_address_geocodes_adopt_keys` preview then execute; record identities, keys, adopted, skipped; verify `SELECT count() FROM se_address_geocodes WHERE address_identity_run_id LIKE 'adopted:%'` and that a sample of adopted rows join a normalized row's location key.
4. [ ] A dry geocode of one page on the host is slice 2b's first fold run; nothing else runs here.
5. [ ] Record in the ledger and spec section 9; amend spec 3.7 and 6 with the location-key ruling; archive the ledger.

## Self-review

- Spec coverage: section 4 (normalizer v2 candidates from the slice-1 readout) Task 1; section 6 (function interface, cache, matcher, fallback, per-run tables, adoption) Tasks 2 to 4; section 3.7 (store as cache, key) Tasks 3 and 4 with the location-key amendment; section 9 slice 2 prod steps for this half in Task 5.
- Placeholders: Task 2 quotes the union statement to move rather than inventing it (it exists in the shadow driver); Task 3's tests are named with their assertions and the module's interface is exact; Task 4 lists the rules and the tuple shape.
- Type consistency: `location_key` is defined in Task 1 and consumed by Tasks 3 and 4; `GeocodeOutcome` fields match the published row's geocode block plus the two versions; `STORE_COLUMNS` order governs both inserts.
