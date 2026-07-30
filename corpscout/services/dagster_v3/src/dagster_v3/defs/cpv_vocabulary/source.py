"""Download and parse the EU's Common Procurement Vocabulary (CPV 2008).

CPV is the classification every EU-directive procurement notice carries, and
the registers publish only the CODE — `45213100` and nothing else. Without the
vocabulary a reader is shown eight digits that name nothing, which is why the
backoffice previously had 45 division labels written by hand and no way to say
anything about the ~9,400 codes beneath them.

This is the authoritative list: Regulation (EC) No 213/2008, published by TED
as an XML archive of 9,454 codes in 23 languages. It has not changed since
2008 — the file itself is dated August 2008 — so this is a full-refresh source
with no schedule, materialised when the table needs building rather than on a
cadence.

Only the English label is kept. The register publishes the code, the code is
the "original" a reader checks against, and English is the page's language;
the other 22 translations are available in the same file if a country page
ever wants them.
"""

from __future__ import annotations

from dataclasses import dataclass
import io
from xml.etree import ElementTree

# TED compresses the archive with DEFLATE64, which the standard library's
# zipfile cannot read -- it raises "That compression method is not supported"
# on the one member that matters. This package registers a decompressor for it
# and is otherwise the same API, so importing it replaces `import zipfile`.
import zipfile_deflate64 as zipfile

CPV_2008_URL = "https://ted.europa.eu/documents/d/ted/cpv_2008_xml"

# The archive also ships `code_cpv_suppl_2008.xml`, the SUPPLEMENTARY
# vocabulary -- alphanumeric codes describing a purchase's nature rather than
# its subject. Loading that as CPV would fill the table with a different
# scheme, so the member is named exactly rather than taken as "the XML file".
CPV_2008_XML_MEMBER = "cpv_2008.xml"

_EN = "EN"


@dataclass(frozen=True)
class CpvCode:
    """One node of the CPV tree."""

    code: str
    label_en: str
    # How many leading digits carry meaning. CPV is read left to right and
    # trailing zeros mean "no more detail", so this is the node's depth AND the
    # length of the prefix every descendant shares.
    significant_digits: int
    # The nearest ancestor that actually exists in the vocabulary, or '' for a
    # division. Not simply "one digit shorter": the levels are not uniform, and
    # a parent that is not itself a code would break the tree.
    parent_code: str


def _significant_digits(code: str) -> int:
    # Never below two: 30000000 strips to '3', which is not a division, so the
    # division 30 is the coarsest thing it can mean.
    return max(2, len(code.rstrip("0")))


def extract_cpv_xml(archive_bytes: bytes) -> bytes:
    """The main vocabulary out of the downloaded archive."""
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        names = set(archive.namelist())
        if CPV_2008_XML_MEMBER not in names:
            raise ValueError(
                f"{CPV_2008_XML_MEMBER} missing from the CPV archive; found {sorted(names)}"
            )
        return archive.read(CPV_2008_XML_MEMBER)


def parse_cpv_vocabulary(xml_bytes: bytes) -> tuple[CpvCode, ...]:
    """Every CPV code with its English label, placed in the tree.

    Raises rather than returning nothing when the document holds no codes: an
    empty parse must not be allowed to replace a populated table.
    """
    root = ElementTree.fromstring(xml_bytes)

    labelled: list[tuple[str, str]] = []
    for element in root:
        # `45000000-7` in the XML; the check digit after the dash is not part of
        # what any register publishes.
        code = str(element.get("CODE") or "").split("-")[0].strip()
        if not code:
            continue
        label = next(
            (
                (text.text or "").strip()
                for text in element
                if text.get("LANG") == _EN and (text.text or "").strip()
            ),
            "",
        )
        # A code with no English label is skipped rather than stored blank: an
        # empty label is worse than an honest absence, because the UI would
        # render a nameless row that looks like a bug in the data.
        if label:
            labelled.append((code, label))

    if not labelled:
        raise ValueError("no CPV codes parsed from the vocabulary document")

    known = {code for code, _ in labelled}

    rows: list[CpvCode] = []
    for code, label in labelled:
        depth = _significant_digits(code)
        parent = ""
        # Walk up one significant digit at a time and take the first ancestor
        # that is a real code. The vocabulary skips levels -- 45213000 does not
        # exist though 45213100 does -- so the naive "one shorter" parent would
        # dangle.
        for candidate_depth in range(depth - 1, 1, -1):
            candidate = code[:candidate_depth].ljust(8, "0")
            if candidate != code and candidate in known:
                parent = candidate
                break
        rows.append(
            CpvCode(
                code=code,
                label_en=label,
                significant_digits=depth,
                parent_code=parent,
            )
        )

    return tuple(rows)
