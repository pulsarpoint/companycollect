from datetime import date
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator


class CompanySearchResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    beliggenhedsadresse: str
    by: str | None
    co_navn: str | None = Field(alias="coNavn")
    cvr: str
    email: str | None
    enhedsnummer: str
    enhedstype: Literal["virksomhed"]
    har_pseudo_cvr: bool = Field(alias="harPseudoCvr")
    highlight_binavn: bool = Field(alias="highlightBinavn")
    highlight_historisk_binavn: bool = Field(alias="highlightHistoriskBinavn")
    highlight_historisk_hovednavn: bool = Field(alias="highlightHistoriskHovednavn")
    hovedbranche: str | None
    ophoers_dato: date | None = Field(alias="ophoersDato")
    postnummer: str | None
    reg: str | None
    reklame_beskyttet: bool = Field(alias="reklameBeskyttet")
    seneste_navn: str = Field(alias="senesteNavn")
    start_dato: date = Field(alias="startDato")
    status: str
    telefonnummer: str | None
    virksomhedsform: str
    vis_navn_postfix: bool = Field(alias="visNavnPostfix")

    @field_validator("ophoers_dato", mode="before")
    @classmethod
    def normalize_empty_ophoers_dato(cls, value: object) -> object:
        if value == "":
            return None
        return value


class PersonSearchResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    aktive_tilknytninger: list[JsonValue] = Field(alias="aktiveTilknytninger")
    beliggenhedsadresse: str
    by: str | None
    co_navn: str | None = Field(alias="coNavn")
    enhedsnummer: str
    enhedstype: Literal["person"]
    har_aktive_relationer: bool = Field(alias="harAktiveRelationer")
    person_type: str = Field(alias="personType")
    postnummer: str | None
    seneste_navn: str = Field(alias="senesteNavn")
    tilknytning: list[JsonValue]


class ProductionUnitSearchResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    beliggenhedsadresse: str
    by: str | None
    co_navn: str | None = Field(alias="coNavn")
    email: str | None
    enhedstype: Literal["produktionsenhed"]
    hovedbranche: str
    ophoers_dato: date | None = Field(alias="ophoersDato")
    p_nummer: str = Field(alias="pNummer")
    postnummer: str | None
    reklame_beskyttet: bool = Field(alias="reklameBeskyttet")
    seneste_navn: str = Field(alias="senesteNavn")
    start_dato: date = Field(alias="startDato")
    status: str
    telefonnummer: str | None

    @field_validator("ophoers_dato", mode="before")
    @classmethod
    def normalize_empty_ophoers_dato(cls, value: object) -> object:
        if value == "":
            return None
        return value


type SearchResultUnit = Annotated[
    CompanySearchResult | PersonSearchResult | ProductionUnitSearchResult,
    Field(discriminator="enhedstype"),
]


class SearchResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    enheder: list[SearchResultUnit]
    p_enhed_total: int = Field(alias="pEnhedTotal", ge=0)
    person_total: int = Field(alias="personTotal", ge=0)
    total: int = Field(ge=0)
    virksomhed_total: int = Field(alias="virksomhedTotal", ge=0)
