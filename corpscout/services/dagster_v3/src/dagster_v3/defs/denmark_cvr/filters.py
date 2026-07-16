from dataclasses import dataclass
from datetime import date

DATACVR_RESULT_LIMIT = 3_000

DATACVR_REGIONS: list[tuple[str, str]] = [
    ("0", "Grønland"),
    ("29190623", "Region Hovedstaden"),
    ("29190925", "Region Midtjylland"),
    ("29190941", "Region Nordjylland"),
    ("29190658", "Region Sjælland"),
    ("29190909", "Region Syddanmark"),
]

# DataCVR exposes municipality and region as independent filters. Keeping the valid
# pair together avoids issuing the full 6 x 105 cross-product for every large month.
DATACVR_MUNICIPALITIES: list[tuple[str, str, str]] = [
    ("0", "955", "Kujalleq"),
    ("0", "956", "Sermersooq"),
    ("0", "957", "Qeqqata"),
    ("0", "959", "Qeqertalik"),
    ("0", "960", "Avannaata"),
    ("0", "961", "Grønland"),
    ("29190623", "101", "København"),
    ("29190623", "147", "Frederiksberg"),
    ("29190623", "151", "Ballerup"),
    ("29190623", "153", "Brøndby"),
    ("29190623", "155", "Dragør"),
    ("29190623", "157", "Gentofte"),
    ("29190623", "159", "Gladsaxe"),
    ("29190623", "161", "Glostrup"),
    ("29190623", "163", "Herlev"),
    ("29190623", "165", "Albertslund"),
    ("29190623", "167", "Hvidovre"),
    ("29190623", "169", "Høje Taastrup"),
    ("29190623", "173", "Lyngby-Taarbæk"),
    ("29190623", "175", "Rødovre"),
    ("29190623", "183", "Ishøj"),
    ("29190623", "185", "Tårnby"),
    ("29190623", "187", "Vallensbæk"),
    ("29190623", "190", "Furesø"),
    ("29190623", "201", "Allerød"),
    ("29190623", "210", "Fredensborg"),
    ("29190623", "217", "Helsingør"),
    ("29190623", "219", "Hillerød"),
    ("29190623", "223", "Hørsholm"),
    ("29190623", "230", "Rudersdal"),
    ("29190623", "240", "Egedal"),
    ("29190623", "250", "Frederikssund"),
    ("29190623", "260", "Halsnæs"),
    ("29190623", "270", "Gribskov"),
    ("29190623", "400", "Bornholm"),
    ("29190623", "411", "Christiansø"),
    ("29190925", "615", "Horsens"),
    ("29190925", "657", "Herning"),
    ("29190925", "661", "Holstebro"),
    ("29190925", "665", "Lemvig"),
    ("29190925", "671", "Struer"),
    ("29190925", "706", "Syddjurs"),
    ("29190925", "707", "Norddjurs"),
    ("29190925", "710", "Favrskov"),
    ("29190925", "727", "Odder"),
    ("29190925", "730", "Randers"),
    ("29190925", "740", "Silkeborg"),
    ("29190925", "741", "Samsø"),
    ("29190925", "746", "Skanderborg"),
    ("29190925", "751", "Aarhus"),
    ("29190925", "756", "Ikast-Brande"),
    ("29190925", "760", "Ringkøbing-Skjern"),
    ("29190925", "766", "Hedensted"),
    ("29190925", "779", "Skive"),
    ("29190925", "791", "Viborg"),
    ("29190941", "773", "Morsø"),
    ("29190941", "787", "Thisted"),
    ("29190941", "810", "Brønderslev"),
    ("29190941", "813", "Frederikshavn"),
    ("29190941", "820", "Vesthimmerlands"),
    ("29190941", "825", "Læsø"),
    ("29190941", "840", "Rebild"),
    ("29190941", "846", "Mariagerfjord"),
    ("29190941", "849", "Jammerbugt"),
    ("29190941", "851", "Aalborg"),
    ("29190941", "860", "Hjørring"),
    ("29190658", "253", "Greve"),
    ("29190658", "259", "Køge"),
    ("29190658", "265", "Roskilde"),
    ("29190658", "269", "Solrød"),
    ("29190658", "306", "Odsherred"),
    ("29190658", "316", "Holbæk"),
    ("29190658", "320", "Faxe"),
    ("29190658", "326", "Kalundborg"),
    ("29190658", "329", "Ringsted"),
    ("29190658", "330", "Slagelse"),
    ("29190658", "336", "Stevns"),
    ("29190658", "340", "Sorø"),
    ("29190658", "350", "Lejre"),
    ("29190658", "360", "Lolland"),
    ("29190658", "370", "Næstved"),
    ("29190658", "376", "Guldborgsund"),
    ("29190658", "390", "Vordingborg"),
    ("29190909", "410", "Middelfart"),
    ("29190909", "420", "Assens"),
    ("29190909", "430", "Faaborg-Midtfyn"),
    ("29190909", "440", "Kerteminde"),
    ("29190909", "450", "Nyborg"),
    ("29190909", "461", "Odense"),
    ("29190909", "479", "Svendborg"),
    ("29190909", "480", "Nordfyns"),
    ("29190909", "482", "Langeland"),
    ("29190909", "492", "Ærø"),
    ("29190909", "510", "Haderslev"),
    ("29190909", "530", "Billund"),
    ("29190909", "540", "Sønderborg"),
    ("29190909", "550", "Tønder"),
    ("29190909", "561", "Esbjerg"),
    ("29190909", "563", "Fanø"),
    ("29190909", "573", "Varde"),
    ("29190909", "575", "Vejen"),
    ("29190909", "580", "Aabenraa"),
    ("29190909", "607", "Fredericia"),
    ("29190909", "621", "Kolding"),
    ("29190909", "630", "Vejle"),
]


@dataclass(frozen=True)
class DenmarkCvrQueryFilter:
    start_date: date
    end_date: date
    region: str
    municipality: str

    def __post_init__(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("DataCVR start date must not exceed its end date")
        if (self.region == "") != (self.municipality == ""):
            raise ValueError(
                "DataCVR region and municipality must either both be set or both be blank"
            )

    @property
    def filter_id(self) -> str:
        if self.municipality == "":
            return "all-companies"
        return f"region-{self.region}-municipality-{self.municipality}"


def filters_for_month(
    *,
    start_date: date,
    end_date: date,
    advertised_count: int,
) -> tuple[DenmarkCvrQueryFilter, ...]:
    if advertised_count < 0:
        raise ValueError("DataCVR advertised count must not be negative")
    if advertised_count <= DATACVR_RESULT_LIMIT:
        return (
            DenmarkCvrQueryFilter(
                start_date=start_date,
                end_date=end_date,
                region="",
                municipality="",
            ),
        )
    return tuple(
        DenmarkCvrQueryFilter(
            start_date=start_date,
            end_date=end_date,
            region=region,
            municipality=municipality,
        )
        for region, municipality, _ in DATACVR_MUNICIPALITIES
    )
