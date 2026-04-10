import tomllib
from pathlib import Path

from pydantic import Field, TypeAdapter
from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class Output:
    directory: Path


@dataclass(frozen=True)
class Datasource:
    connection: str


@dataclass(frozen=True)
class Publishing:
    publisher: str
    base_uri: str
    version: str = ""
    publisher_uid: str = ""
    publisher_scheme: str = ""
    publisher_uri: str = ""
    license: str = ""
    publication_policy: str = Field(default="", alias="publicationPolicy")


@dataclass(frozen=True)
class Mapping:
    file: Path
    ocid_prefix: str
    selector: str
    force_publish: bool | None = False
    codelists: Path | None = None
    #: SQL query to load milestone code metadata. Must return ``code``, ``title`` and ``description`` columns.
    #: Used to enrich milestones with titles and descriptions, and to identify known codes for deduplication.
    milestone_lookup_sql: str | None = None
    #: Whether to split space-separated milestone codes into individual milestone objects. When enabled, a value
    #: like ``"CA AT AU"`` in the milestone code field produces three separate milestones. Requires the source data
    #: to encode multiple codes in a single field; do not enable if milestone codes can legitimately contain spaces.
    split_milestone_codes: bool = False
    #: OCDS paths at which to keep source values that aren't in the codelist, instead of discarding them.
    #: Useful when values are derived via SQL logic (e.g. CASE expressions) and are absent from the codelist file.
    codelist_passthrough_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class Config:
    datasource: Datasource
    mapping: Mapping
    publishing: Publishing
    output: Output

    @classmethod
    def from_file(cls, config_file: Path) -> "Config":
        with config_file.open("rb") as f:
            data = tomllib.load(f)
        return TypeAdapter(Config).validate_python(data)
