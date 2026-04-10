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
