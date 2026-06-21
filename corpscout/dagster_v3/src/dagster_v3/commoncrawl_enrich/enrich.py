import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from dagster_v3.commoncrawl_enrich import extract, tech
from dagster_v3.commoncrawl_enrich.llm import LLMArm
from dagster_v3.commoncrawl_enrich.models import DomainEnrichment, DomainTarget, FetchedPage, IndexRecord

LOGGER = logging.getLogger(__name__)

ResolveFn = Callable[..., dict[str, IndexRecord]]
FetchFn = Callable[..., FetchedPage | None]


def _enrich_one(target: DomainTarget, record: IndexRecord | None, *, fetch: FetchFn, llm: LLMArm) -> DomainEnrichment:
    if record is None:
        return DomainEnrichment(target=target, fetch_status="not_in_index")
    page = fetch(record)
    if page is None:
        return DomainEnrichment(target=target, fetch_status="fetch_failed")

    det = extract.extract_deterministic(page)
    enr = DomainEnrichment(
        target=target, fetch_status="ok", page=page,
        title=det.title, meta_description=det.meta_description, content_language=det.content_language,
        ico=det.ico, dic=det.dic, ico_checksum_valid=det.ico_checksum_valid,
        emails=list(det.emails), phones=list(det.phones), socials=list(det.socials),
        technologies=tech.detect_technologies(page.html, page.headers),
    )
    parsed_text = extract.parse_html(page.html).text
    enr.industry = llm.classify_industry(parsed_text)
    if not enr.emails and not enr.phones:  # contact-recall only on the deterministic-miss residual
        extra_emails, extra_phones = llm.recover_contacts(parsed_text)
        enr.emails.extend(extra_emails)
        enr.phones.extend(extra_phones)
    return enr


def enrich_domains(targets: list[DomainTarget], *, resolve: ResolveFn, fetch: FetchFn,
                   llm: LLMArm, crawl_id: str, max_workers: int = 16) -> list[DomainEnrichment]:
    records = resolve([t.root_domain for t in targets], crawl_id=crawl_id)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(
            lambda t: _enrich_one(t, records.get(t.root_domain), fetch=fetch, llm=llm),
            targets,
        ))
