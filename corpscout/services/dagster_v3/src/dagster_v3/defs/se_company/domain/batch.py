"""Bounded source reads, current-input verification and history-before-main publication."""

from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from contextlib import ExitStack, closing
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

from dagster_v3.defs.se_company.basic_info.extract import scope_pages
from dagster_v3.defs.se_company.domain import tables
from dagster_v3.defs.se_company.domain.evidence import digest, input_payload, json_text, requires_verification
from dagster_v3.defs.se_company.domain.fold import FOLD_VERSION, effective_rank, fold_company
from dagster_v3.defs.se_company.domain.verification import DomainVerificationProfile, fingerprints, revalidate_saved_response, verify_domain
from dagster_v3.defs.se_company.info import build_llm_client

QUERY_SETTINGS = {"max_query_size": 1_048_576, "max_execution_time": 1800}


def read_rows(client: Any, table: str, columns: Sequence[str], ids: Sequence[str]) -> list[dict[str, Any]]:
    return [dict(zip(columns, row, strict=True)) for row in client.execute(
        f"SELECT {', '.join(columns)} FROM corpscout.{table} FINAL WHERE company_id IN %(company_ids)s",
        {"company_ids": tuple(ids)}, settings=QUERY_SETTINGS,
    )]


def insert_rows(client: Any, table: str, columns: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
    if rows:
        client.execute(f"INSERT INTO corpscout.{table} ({', '.join(columns)}) VALUES",
                       [tuple(row[column] for column in columns) for row in rows])


def fold_input_hash(
    company: Mapping[str, Any], suggestions: Sequence[Mapping[str, Any]],
    precedence: Sequence[Mapping[str, Any]], rules: Sequence[Mapping[str, Any]],
    verified: Mapping[str, Mapping[str, Any]],
) -> str:
    # Source timestamps and run IDs cannot trigger processing. Rules include their actual
    # decisions, not their write time; prompt/model content is represented by input_hash.
    def stable(rows: Sequence[Mapping[str, Any]], ignored: set[str]) -> list[str]:
        return sorted(json_text({k: v for k, v in row.items() if k not in ignored}) for row in rows)

    return digest(json_text({
        "version": FOLD_VERSION, "company": company,
        "suggestions": stable(suggestions, {"observed_at", "suggested_at", "suggestion_id", "source_run_id", "extractor_version"}),
        "precedence": stable(precedence, {"decided_at"}), "rules": stable(rules, set()),
        "verification": stable(list(verified.values()), {"verified_at", "source_run_id", "raw_response", "prompt_tokens", "completion_tokens", "prompt_version"}),
    }))


def processing_scope_sql() -> str:
    return "SELECT DISTINCT company_id FROM (" + " UNION ALL ".join(
        f"SELECT company_id FROM corpscout.{table}" for table in (
            tables.SUGGESTION_TABLE, tables.MAIN_TABLE, tables.RULE_TABLE,
        )
    ) + ")"


def read_domain_inputs(client: Any, ids: Sequence[str]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    groups = {}
    for table, columns in (
        (tables.SUGGESTION_TABLE, tables.SUGGESTION_COLUMNS),
        (tables.PRECEDENCE_TABLE, tables.PRECEDENCE_COLUMNS), (tables.RULE_TABLE, tables.RULE_COLUMNS),
        ("se_company_basic_info", ("company_id", "legal_name", "lei", "wikidata_id")),
    ):
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in read_rows(client, table, columns, ids):
            grouped[row["company_id"]].append(row)
        groups[table] = grouped
    return groups


def verification_requests(
    ids: Sequence[str], groups: Mapping[str, Mapping[str, list[dict[str, Any]]]],
    globals_: list[dict[str, Any]], profile: DomainVerificationProfile | None,
) -> dict[tuple[str, str], tuple[str, dict[str, str]]]:
    requests = {}
    for company_id in ids:
        identity = groups["se_company_basic_info"][company_id]
        company = identity[0] if identity else {"company_id": company_id}
        domains: dict[str, list[dict[str, Any]]] = defaultdict(list)
        reviewed = {r["root_domain"] for r in groups[tables.RULE_TABLE][company_id]
                    if not r["removed"] and r["action"] != "unreviewed"}
        for row in groups[tables.SUGGESTION_TABLE][company_id]:
            if not row["removed"]:
                domains[row["root_domain"]].append(row)
        precedence = globals_ + groups[tables.PRECEDENCE_TABLE][company_id]
        for domain, suggestions in sorted(domains.items()):
            suggestions = [r for r in suggestions if effective_rank("association", r["source"], domain, precedence) > 0]
            reviewer_claim = any(r["source"] == "reviewer" and r["association"] != "uncertain" and r["confidence"] >= .9 for r in suggestions)
            if domain in reviewed or reviewer_claim or not requires_verification(suggestions):
                continue
            payload = input_payload(company, domain, suggestions)
            requests[company_id, domain] = (payload, fingerprints(payload, profile))
    return requests


def read_verifications(
    client: Any, requests: Mapping[tuple[str, str], tuple[str, dict[str, str]]], *, profile: DomainVerificationProfile | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    cache = {}
    if not requests:
        return cache
    hash_column = "input_hash" if profile is not None else "data_hash"
    accepted_hashes = {key: {hashes[hash_column]} for key, (_, hashes) in requests.items()}
    if profile is not None and profile.max_tokens is None:
        # Preserve paid answers from the retired backoffice cap. This hash is only
        # for reading history; requests use the uncapped profile. Every other
        # evidence, prompt and model setting must still match exactly.
        previous_profile = profile.model_copy(update={"max_tokens": 4_000})
        for key, (payload, _) in requests.items():
            accepted_hashes[key].add(fingerprints(payload, previous_profile)["input_hash"])
    # MergeTree keeps every attempt. The latest matching attempt wins;
    # an HTTP failure never silently resurrects an older successful forced retry.
    rows = client.execute(
        f"SELECT {', '.join(tables.VERIFICATION_COLUMNS)} FROM corpscout.{tables.VERIFICATION_TABLE} "
        f"WHERE company_id IN %(company_ids)s AND {hash_column} IN %(hashes)s "
        "ORDER BY verified_at DESC LIMIT 1 BY company_id, root_domain",
        {"company_ids": tuple(sorted({company for company, _ in requests})),
         "hashes": tuple(sorted({value for values in accepted_hashes.values() for value in values}))}, settings=QUERY_SETTINGS,
    )
    for values in rows:
        record = dict(zip(tables.VERIFICATION_COLUMNS, values, strict=True))
        key = record["company_id"], record["root_domain"]
        if key in accepted_hashes and record[hash_column] in accepted_hashes[key]:
            cache[key] = revalidate_saved_response(record)
    return cache


def verify_domains(
    client: Any, *, page_size: int, changed_only: bool, profile: DomainVerificationProfile | None,
    max_llm_calls: int | None, run_id: str, log: Callable[..., None], retry_failed_only: bool,
    llm_factory: Callable[..., Any] = build_llm_client,
) -> Counter[str]:
    """Score eligible suggestions and persist every paid attempt; never publish domains."""
    counts: Counter[str] = Counter()
    if profile is None:
        return counts
    started = perf_counter()
    last_progress = started
    call_limit = max_llm_calls if max_llm_calls is not None else "unlimited"
    log("Domain verification starting: changed_only=%s retry_failed_only=%s max_llm_calls=%s; loading current inputs",
        changed_only, retry_failed_only, call_limit)
    with ExitStack() as stack:
        llm = None
        globals_ = read_rows(client, tables.PRECEDENCE_TABLE, tables.PRECEDENCE_COLUMNS, [""])
        pages = stack.enter_context(closing(scope_pages(
            client, scope_sql=processing_scope_sql(), params={}, page_size=page_size,
            settings=QUERY_SETTINGS, prefix="corpscout._tmp_domain_verify_",
        )))
        for ids in pages:
            counts["pages"] += 1
            counts["companies"] += len(ids)
            requests = verification_requests(ids, read_domain_inputs(client, ids), globals_, profile)
            counts["eligible_associations"] += len(requests)
            cache = read_verifications(client, requests, profile=profile)
            for (company_id, domain), (payload, hashes) in requests.items():
                saved = cache.get((company_id, domain))
                if saved is not None and saved.get("recovered_by_parser"):
                    counts["verification_recovered"] += 1
                if saved is not None and saved["status"] == "success" and (changed_only or retry_failed_only):
                    counts["verification_reused"] += 1
                    continue
                if retry_failed_only and saved is None:
                    counts["new_associations_skipped"] += 1
                    continue
                if max_llm_calls is not None and counts["llm_calls"] >= max_llm_calls:
                    counts["verification_pending"] += 1
                    continue
                if llm is None:
                    llm = llm_factory(profile, timeout_seconds=120,
                                      api_key_environment_variable=profile.api_key_environment_variable)
                    stack.callback(llm.close)
                record = verify_domain(llm, company_id=company_id, domain=domain, payload=payload,
                                       profile=profile, hashes=hashes, run_id=run_id)
                insert_rows(client, tables.VERIFICATION_TABLE, tables.VERIFICATION_COLUMNS, [record])
                counts["llm_calls"] += 1
                counts[record["status"]] += 1
                counts["prompt_tokens"] += record["prompt_tokens"]
                counts["completion_tokens"] += record["completion_tokens"]
                if record["status"] != "success":
                    log("Domain verification failed: company_id=%s domain=%s status=%s detail=%s",
                        company_id, domain, record["status"], record["error"].splitlines()[0][:350])
                now = perf_counter()
                if now - last_progress >= 30:
                    log("Domain verification progress: %d/%s LLM calls, %d successful, %d invalid responses, "
                        "%d HTTP errors, %d reused (%d recovered from saved JSON); last company_id=%s domain=%s; elapsed %.1fs",
                        counts["llm_calls"], call_limit, counts["success"], counts["invalid_response"], counts["http_error"],
                        counts["verification_reused"], counts["verification_recovered"], company_id, domain, now - started)
                    last_progress = now
            log("Domain verification: %d eligible associations, %d LLM calls, %d cached, %d pending",
                counts["eligible_associations"], counts["llm_calls"], counts["verification_reused"], counts["verification_pending"])
    return counts


def publish_domains(
    client: Any, *, page_size: int, changed_only: bool, profile: DomainVerificationProfile | None,
    run_id: str, log: Callable[..., None],
) -> Counter[str]:
    """Fold persisted answers for current inputs; the profile selects cache entries only."""
    started = perf_counter()
    counts: Counter[str] = Counter({key: 0 for key in (
        "pages", "completed_pages", "companies", "unchanged_companies", "domains_written", "history_rows",
        "written_active_domains", "written_withdrawn_domains", "written_rejected_domains", "written_unverified_domains",
        "verification_reused", "verification_recovered", "verification_pending", "verification_success", "verification_http_error", "verification_invalid_response",
    )})
    log("Domain publication starting: run_id=%s changed_only=%s page_size=%d verification_matching=%s",
        run_id, changed_only, page_size, "exact_evidence_prompt_model" if profile is not None else "latest_current_evidence")
    log("Domain publication: loading global source precedence")
    globals_ = read_rows(client, tables.PRECEDENCE_TABLE, tables.PRECEDENCE_COLUMNS, [""])
    log("Domain publication: loaded %d global precedence rules in %.2fs; building company scope and fetching the first page",
        len(globals_), perf_counter() - started)
    scope_started = perf_counter()
    with closing(scope_pages(
        client, scope_sql=processing_scope_sql(), params={}, page_size=page_size,
        settings=QUERY_SETTINGS, prefix="corpscout._tmp_domain_publish_",
    )) as pages:
        for ids in pages:
            page_started = perf_counter()
            counts["pages"] += 1
            page = counts["pages"]
            if page == 1:
                log("Domain publication: company scope and first page ready in %.2fs", page_started - scope_started)
            log("Domain publication page %d: loading inputs for %d companies (%s–%s); %d companies completed so far",
                page, len(ids), ids[0], ids[-1], counts["companies"])
            groups = read_domain_inputs(client, ids)
            suggestion_count = sum(len(rows) for rows in groups[tables.SUGGESTION_TABLE].values())
            rule_count = sum(len(rows) for rows in groups[tables.RULE_TABLE].values())
            log("Domain publication page %d: loaded %d suggestions and %d reviewer rules in %.2fs; loading published domains",
                page, suggestion_count, rule_count, perf_counter() - page_started)
            previous_by_company: dict[str, list[dict[str, Any]]] = defaultdict(list)
            previous_started = perf_counter()
            for row in read_rows(client, tables.MAIN_TABLE, tables.MAIN_COLUMNS, ids):
                previous_by_company[row["company_id"]].append(row)
            log("Domain publication page %d: loaded %d published domains in %.2fs; identifying associations requiring verification",
                page, sum(len(rows) for rows in previous_by_company.values()), perf_counter() - previous_started)
            requests = verification_requests(ids, groups, globals_, profile)
            log("Domain publication page %d: loading saved verification results for %d uncertain or conflicting associations",
                page, len(requests))
            verification_started = perf_counter()
            cache = read_verifications(client, requests, profile=profile)
            verified_by_company: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
            verification_statuses: Counter[str] = Counter()
            for company_id, domain in requests:
                saved = cache.get((company_id, domain))
                if saved is None:
                    counts["verification_pending"] += 1
                else:
                    verified_by_company[company_id][domain] = saved
                    counts["verification_reused"] += 1
                    counts["verification_recovered"] += int(bool(saved.get("recovered_by_parser")))
                    verification_statuses[saved["status"]] += 1
            for status, amount in verification_statuses.items():
                counts[f"verification_{status}"] += amount
            recovered = sum(bool(row.get("recovered_by_parser")) for row in cache.values())
            if recovered:
                log("Domain publication page %d: recovered %d previously invalid saved responses with the current parser; no LLM calls", page, recovered)
            log("Domain publication page %d: %d saved verifications (%d successful, %d HTTP errors, %d invalid responses), "
                "%d missing current results in %.2fs; folding company domains",
                page, len(cache), verification_statuses["success"], verification_statuses["http_error"],
                verification_statuses["invalid_response"], len(requests) - len(cache), perf_counter() - verification_started)
            history = []
            output = []
            unchanged_before = counts["unchanged_companies"]
            folding_started = perf_counter()
            last_progress = folding_started
            for index, company_id in enumerate(ids, 1):
                verified = verified_by_company[company_id]
                identity = groups["se_company_basic_info"][company_id]
                suggestions = groups[tables.SUGGESTION_TABLE][company_id]
                previous = previous_by_company[company_id]
                precedence = globals_ + groups[tables.PRECEDENCE_TABLE][company_id]
                rules = groups[tables.RULE_TABLE][company_id]
                signature = fold_input_hash(identity[0] if identity else {"company_id": company_id},
                                            suggestions, precedence, rules, verified)
                if changed_only and previous and all(row["fold_input_hash"] == signature for row in previous):
                    counts["unchanged_companies"] += 1
                else:
                    rows, changes = fold_company(company_id=company_id, suggestions=suggestions, previous=previous,
                                                 precedence=precedence, rules=rules, verification=verified,
                                                 folded_at=datetime.now(UTC), source_run_id=run_id)
                    for row in [*rows, *changes]:
                        row["fold_input_hash"] = signature
                    output.extend(rows)
                    history.extend(changes)
                now = perf_counter()
                if now - last_progress >= 10:
                    log("Domain publication page %d: folded %d/%d companies, through company_id=%s; "
                        "%d unchanged, %d domain rows prepared, %.2fs folding elapsed",
                        page, index, len(ids), company_id, counts["unchanged_companies"] - unchanged_before,
                        len(output), now - folding_started)
                    last_progress = now
            log("Domain publication page %d: folding finished in %.2fs; %d unchanged companies, "
                "%d domain rows prepared, %d domain changes",
                page, perf_counter() - folding_started, counts["unchanged_companies"] - unchanged_before,
                len(output), len(history))
            for change in history[:5]:
                log("Domain publication page %d change sample: company_id=%s domain=%s change=%s "
                    "association=%s active=%d inactive_reason=%s verification=%s",
                    page, change["company_id"], change["root_domain"], change["change_kind"],
                    change["association"], change["active"], change["inactive_reason"] or "none", change["verification_status"])
            if len(history) > 5:
                log("Domain publication page %d: %d further changes included in the history batch", page, len(history) - 5)
            if history:
                write_started = perf_counter()
                log("Domain publication page %d: writing %d history rows to %s", page, len(history), tables.HISTORY_TABLE)
                insert_rows(client, tables.HISTORY_TABLE, tables.HISTORY_COLUMNS, history)
                counts["history_rows"] += len(history)
                log("Domain publication page %d: history write finished in %.2fs", page, perf_counter() - write_started)
            if output:
                write_started = perf_counter()
                log("Domain publication page %d: writing %d domain rows to %s", page, len(output), tables.MAIN_TABLE)
                insert_rows(client, tables.MAIN_TABLE, tables.MAIN_COLUMNS, output)
                counts["domains_written"] += len(output)
                counts["written_active_domains"] += sum(row["active"] for row in output)
                for reason in ("withdrawn", "rejected", "unverified"):
                    counts[f"written_{reason}_domains"] += sum(row["inactive_reason"] == reason for row in output)
                log("Domain publication page %d: domain write finished in %.2fs", page, perf_counter() - write_started)
            counts["companies"] += len(ids)
            counts["completed_pages"] += 1
            log("Domain publication page %d complete in %.2fs: %d companies published or unchanged in total, "
                "%d domains written, %d history rows; elapsed %.2fs; fetching next page",
                page, perf_counter() - page_started, counts["companies"], counts["domains_written"],
                counts["history_rows"], perf_counter() - started)
    if counts["pages"] == 0:
        log("Domain publication: no companies found in the source, published-domain or reviewer-rule tables")
    counts["elapsed_ms"] = round((perf_counter() - started) * 1_000)
    log("Domain publication complete in %.2fs: %d pages, %d companies, %d unchanged companies, "
        "%d domains written (%d active, %d withdrawn, %d rejected, %d unverified), "
        "%d history rows, %d saved verifications reused, %d missing current verification results",
        counts["elapsed_ms"] / 1_000, counts["completed_pages"], counts["companies"], counts["unchanged_companies"],
        counts["domains_written"], counts["written_active_domains"], counts["written_withdrawn_domains"],
        counts["written_rejected_domains"], counts["written_unverified_domains"], counts["history_rows"],
        counts["verification_reused"], counts["verification_pending"])
    return counts
