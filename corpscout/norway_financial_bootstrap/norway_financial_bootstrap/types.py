from enum import StrEnum


class CandidateMarkerStatus(StrEnum):
    DONE = "done"
    FAILED = "failed"
    MISSING = "missing"
