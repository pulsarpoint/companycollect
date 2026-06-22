import re

_ICO_LABEL = re.compile(r"(?:IČO|ICO|IČ|IC)\s*[:. ]*\s*([0-9][0-9  ]{6,14}[0-9])", re.IGNORECASE)
_DIC_LABEL = re.compile(r"(?:DIČ|DIC)\s*[:. ]*\s*([0-9]{9,12})", re.IGNORECASE)


def ico_checksum_valid(ico_digits: str) -> bool:
    digits = re.sub(r"\D", "", ico_digits)
    if len(digits) != 8:
        return False
    weights = (8, 7, 6, 5, 4, 3, 2)
    total = sum(int(digits[i]) * weights[i] for i in range(7))
    remainder = total % 11
    if remainder == 0:
        check = 1
    elif remainder == 1:
        check = 0
    else:
        check = 11 - remainder
    return check == int(digits[7])


def extract_icos(text: str) -> list[str]:
    """Return distinct checksum-valid 8-digit IČOs that appear next to an IČO label."""
    found: list[str] = []
    for match in _ICO_LABEL.finditer(text):
        digits = re.sub(r"\D", "", match.group(1))
        if len(digits) >= 8:
            candidate = digits[:8]
            if ico_checksum_valid(candidate) and candidate not in found:
                found.append(candidate)
    return found


def extract_dic(text: str) -> str:
    match = _DIC_LABEL.search(text)
    return match.group(1) if match else ""
