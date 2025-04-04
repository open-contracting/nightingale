import tomllib
from pathlib import Path

from pydantic import TypeAdapter
from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class Output:
    directory: Path


@dataclass(frozen=True)
class Datasource:
    # TODO: postgresql support?
    # XXX: maybe this should be an uri?
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
    publicationPolicy: str = ""


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
        with open(config_file, "rb") as f:
            data = tomllib.load(f)
        return TypeAdapter(Config).validate_python(data)
