"""Fold one company's domains, preserving all independent sources and reviewer decisions."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.domain import tables
from dagster_v3.defs.se_company.domain.evidence import digest, domain_evidence, json_text, requires_verification
from dagster_v3.defs.se_company.domain.precedence import DOMAIN_PRECEDENCE

FOLD_VERSION = "domain-fold-v2-source-support"
LLM_THRESHOLD = 0.9
COMPARE_COLUMNS = tuple(c for c in tables.MAIN_COLUMNS if c not in (
    "folded_at", "fold_version", "source_run_id", "last_seen_at", "fold_input_hash",
))


def effective_rank(field: str, source: str, domain: str, rules: Sequence[Mapping[str, Any]]) -> int:
    rank = DOMAIN_PRECEDENCE.get(field, {}).get(source, 0)
    # Global, then company, then domain scope. Released rows reveal the parent rank.
    for row in sorted(rules, key=lambda row: (row["company_id"] != '', row["root_domain"] != '')):
        if row["removed"] == 0 and row["field"] == field and row["source"] == source and row["root_domain"] in ('', domain):
            rank = int(row["precedence"])
    return rank


def winner(field: str, domain: str, rows: Sequence[Mapping[str, Any]], rules: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    eligible = [row for row in rows if effective_rank(field, row["source"], domain, rules) > 0]
    return min(eligible, key=lambda row: (-effective_rank(field, row["source"], domain, rules), -row["confidence"], row["source"], row["slot"])) if eligible else None


def fold_company(
    *, company_id: str, suggestions: Sequence[Mapping[str, Any]], previous: Sequence[Mapping[str, Any]],
    precedence: Sequence[Mapping[str, Any]], rules: Sequence[Mapping[str, Any]],
    verification: Mapping[str, Mapping[str, Any]], folded_at: datetime, source_run_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Verification must already be restricted to the exact current input hash by the caller."""
    by_domain: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for suggestion in suggestions:
        if suggestion["removed"] == 0 and suggestion["source"] != "reviewer_draft":
            by_domain[suggestion["root_domain"]].append(suggestion)
    old = {row["root_domain"]: row for row in previous}
    decisions = {row["root_domain"]: row for row in rules if not row["removed"]}
    output = []
    primary_scores = {}
    for domain in sorted(set(by_domain) | set(old) | set(decisions)):
        candidates = sorted(by_domain[domain], key=lambda row: (row["source"], row["slot"]))
        before = old.get(domain)
        decision = decisions.get(domain)
        website = winner("website", domain, [r for r in candidates if r["website_url"]], precedence)
        # An uncertain mention has no positive/negative opinion to overrule a strong claim.
        association = winner("association", domain, [r for r in candidates if r["association"] != "uncertain" and r["confidence"] >= LLM_THRESHOLD], precedence)
        eligible = [r for r in candidates if effective_rank("association", r["source"], domain, precedence) > 0]
        if (association is None or association["source"] != "reviewer") and requires_verification(eligible):
            association = None
        supporting = [r for r in eligible if r["source"] in tables.EXTRACTOR_SOURCES and r["association"] != "not_connected" and effective_rank("primary", r["source"], domain, precedence) > 0]
        supporting_sources = sorted({r["source"] for r in supporting})
        strongest = winner("primary", domain, supporting, precedence)
        primary = winner("primary", domain, [r for r in supporting if r["is_primary"]], precedence)
        reviewer_primary = any(r["source"] == "reviewer" and r["is_primary"] and r["association"] == "connected" and r["confidence"] >= LLM_THRESHOLD for r in eligible)
        if not candidates and before is None and (decision is None or decision["action"] == "rejected"):
            continue
        evidence_hash = digest(json_text(domain_evidence(candidates)))
        if candidates:
            provenance = {
                "sources": [r["source"] for r in candidates],
                "source_confidences": [r["confidence"] for r in candidates],
                "source_record_ids": [r["source_record_id"] for r in candidates],
                "source_urls": [r["source_url"] for r in candidates],
                "confidence_bases": [r["confidence_basis"] for r in candidates],
            }
        elif before is not None:
            provenance = {key: before[key] for key in ("sources", "source_confidences", "source_record_ids", "source_urls", "confidence_bases")}
        else:
            provenance = {"sources": ["reviewer"], "source_confidences": [1.0], "source_record_ids": [""], "source_urls": [""], "confidence_bases": ["reviewer_confirmation"]}
        row = {
            "company_id": company_id, "root_domain": domain,
            "website_url": website["website_url"] if website else (before["website_url"] if before else f"https://{domain}"),
            "website_host": website["website_host"] if website else (before["website_host"] if before else domain),
            "website_source": website["source"] if website else (before["website_source"] if before else "reviewer"),
            "association": association["association"] if association else "uncertain",
            "association_source": association["source"] if association else "",
            "is_primary": 0, "primary_source": "",
            "confidence": float(association["confidence"] if association else max((r["confidence"] for r in candidates), default=0)),
            **provenance, "supporting_sources": supporting_sources, "evidence_hash": evidence_hash,
            "verification_status": "not_requested", "verification_reason": "", "verification_input_hash": "",
            "review_status": "unreviewed", "review_note": "", "reviewed_by": "", "reviewed_at": None, "reviewed_evidence_hash": "",
            "active": 0, "inactive_reason": "",
            "first_seen_at": before["first_seen_at"] if before else folded_at,
            "last_seen_at": max((r["observed_at"] for r in candidates), default=before["last_seen_at"] if before else folded_at),
            "folded_at": folded_at, "fold_version": FOLD_VERSION, "fold_input_hash": "", "source_run_id": source_run_id,
        }
        verified = verification.get(domain)
        if verified is not None and (association is None or association["source"] != "reviewer"):
            row.update(verification_status=verified["status"], verification_reason=verified["reason"], verification_input_hash=verified["input_hash"])
            if verified["status"] == "success" and verified["confidence"] >= LLM_THRESHOLD and verified["verdict"] != "uncertain":
                row.update(association=verified["verdict"], association_source="llm", confidence=float(verified["confidence"]))
            elif association is None:
                row.update(association="uncertain", association_source="")
        if decision is not None:
            action = decision["action"]
            row.update(review_status=action, review_note=decision["note"], reviewed_by=decision["decided_by"],
                       reviewed_at=decision["decided_at"], reviewed_evidence_hash=decision["evidence_hash"])
            if action != "unreviewed":
                row.update(association="not_connected" if action == "rejected" else "connected", association_source="reviewer", confidence=1.0)
        confirmed = decision is not None and decision["action"] in ("confirmed_primary", "confirmed_related")
        row["active"] = int(row["association"] == "connected" and (bool(candidates) or confirmed))
        row["inactive_reason"] = "" if row["active"] else "rejected" if row["association"] == "not_connected" else "withdrawn" if not candidates else "unverified"
        primary_scores[domain] = (
            not ((decision is not None and decision["action"] == "confirmed_primary") or (decision is None and reviewer_primary)),
            -len(supporting_sources),
            -effective_rank("primary", strongest["source"], domain, precedence) if strongest else 0,
            primary is None,
            -row["confidence"], domain,
        )
        row["primary_source"] = "reviewer" if (confirmed and decision["action"] == "confirmed_primary") or (decision is None and reviewer_primary) else strongest["source"] if strongest else row["association_source"]
        output.append(row)
    active = [r for r in output if r["active"]]
    if active:
        min(active, key=lambda row: primary_scores[row["root_domain"]])["is_primary"] = 1
    history = []
    for row in output:
        before = old.get(row["root_domain"])
        changed = [column for column in COMPARE_COLUMNS if before is None or row[column] != before[column]]
        if changed:
            kind = "created" if before is None else "reactivated" if row["active"] and not before["active"] else row["inactive_reason"] if not row["active"] and before["active"] else "updated"
            history.append({**row, "changed_fields": changed, "changed_at": folded_at, "change_kind": kind, "fold_run_id": source_run_id})
    return output, history
