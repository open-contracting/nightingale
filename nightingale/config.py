import tomllib
from pathlib import Path
from typing import Optional, Self

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
    # selector: str


@dataclass(frozen=True)
class Publishing:
    ocid_prefix: str
    version: str
    publisher: str


@dataclass(frozen=True)
class Mapping:
    name: str
    file: Path
    selector: str
    force_publish: Optional[bool] = False


@dataclass(frozen=True)
class Config:
    datasource: Datasource
    steps: list[Mapping]
    publishing: Publishing
    output: Output

    @classmethod
    def from_file(cls, config_file: Path) -> Self:
        with open(config_file, "rb") as f:
            data = tomllib.load(f)
        return TypeAdapter(Config).validate_python(data)
