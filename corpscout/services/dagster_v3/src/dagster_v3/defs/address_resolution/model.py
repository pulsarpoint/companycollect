from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AddressResolutionPolicy:
    version: str
    minimum_fuzzy_street_length: int
    maximum_street_edit_distance: int
    minimum_decisive_score_margin: float
    site_maximum_spread_meters: float
    area_maximum_spread_meters: float
    exact_score: float
    locality_fallback_score: float
    postcode_mismatch_score: float
    country_fallback_score: float
    fuzzy_postcode_score: float
    fuzzy_locality_score: float
    street_without_house_score: float
    street_missing_requested_house_score: float


@dataclass(frozen=True, slots=True)
class GoldenAddressDocument:
    document_id: str
    country_code: str
    raw_address: str
    search_text: str
    street_name: str
    house_number: str
    unit: str
    postal_code: str
    locality: str
    address_kind: str
    reference_precision: str
    latitude: float | None
    longitude: float | None
    coordinate_spread_meters: float | None
    supporting_record_count: int
    source_record_id: str
    source_record_url: str


@dataclass(frozen=True, slots=True)
class GoldenAddressResolutionCase:
    case_id: str
    description: str
    query: GoldenAddressDocument
    references: tuple[GoldenAddressDocument, ...]
    expected_status: str
    expected_precision: str
    expected_strategy: str


@dataclass(frozen=True, slots=True)
class GoldenAddressResolutionFailure:
    case_id: str
    expected_status: str
    actual_status: str
    expected_precision: str
    actual_precision: str
    expected_strategy: str
    actual_strategy: str


@dataclass(frozen=True, slots=True)
class GoldenAddressResolutionEvaluation:
    corpus_version: str
    policy_version: str
    case_count: int
    passed_count: int
    failures: tuple[GoldenAddressResolutionFailure, ...]

    @property
    def pass_rate_percent(self) -> float:
        if self.case_count == 0:
            return 0.0
        return 100.0 * self.passed_count / self.case_count
