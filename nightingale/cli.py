import logging
import tomllib
import traceback

import click
import click_pathlib
from pydantic import TypeAdapter

from .config import Config
from .loader import DataLoader
from .mapper import OCDSDataMapper
from .publisher import DataPublisher
from .writer import DataWriter

logger = logging.getLogger(__name__)


def setup_logging(loglevel):
    """Configure logging based on the provided log level."""
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logging.basicConfig(
        level=getattr(logging, loglevel.upper()),
        handlers=[handler],
    )


def load_config(config_file):
    """
    Load configuration data from a TOML file.

    :param config_file: Path to the configuration file.
    :return: Dictionary with configuration data.
    """
    try:
        with open(config_file, "rb") as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError:
        raise click.ClickException(f"Error decoding TOML from {config_file}.")


@click.command()
@click.option(
    "--config",
    "config_file",
    help="Path to the configuration file",
    type=click_pathlib.Path(exists=True),
    required=True,
)
@click.option("--package", is_flag=True, default=False, help="Package data")
@click.option("--validate-mapping", is_flag=True, default=False, help="Validate mapping template")
@click.option(
    "--loglevel",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    default="INFO",
    help="Set the logging level",
)
@click.option("--datasource", type=str, help="Datasource connection string")
@click.option("--mapping-file", type=click_pathlib.Path(exists=True), help="Mapping file path")
@click.option("--codelists-file", type=click_pathlib.Path(exists=True), help="Codelists mapping file path")
@click.option("--ocid-prefix", type=str, help="OCID prefix")
@click.option("--selector", type=click_pathlib.Path(exists=True), help="Path to selector SQL script")
@click.option("--force-publish", is_flag=True, help="Force publish")
@click.option("--publisher", type=str, help="Publisher name")
@click.option("--base-uri", type=str, help="Package base URI")
@click.option("--version", type=str, help="OCDS Version")
@click.option("--publisher-uid", type=str, help="Publisher UID")
@click.option("--publisher-scheme", type=str, help="Publisher scheme")
@click.option("--publisher-uri", type=str, help="Publisher URI")
@click.option("--extensions", type=str, multiple=True, help="List of extensions")
@click.option("--output-directory", type=click_pathlib.Path(exists=True), help="Output directory")
def run(
    config_file,
    package,
    validate_mapping,
    loglevel,
    datasource,
    mapping_file,
    codelists_file,
    ocid_prefix,
    selector,
    force_publish,
    publisher,
    base_uri,
    version,
    publisher_uid,
    publisher_scheme,
    publisher_uri,
    extensions,
    output_directory,
):
    """
    Run the data transformation process.

    :param config_file: Path to the configuration file.
    :param package: Flag to indicate whether to package the data.
    :param validate_mapping: Flag to indicate whether to validate the mapping template.
    :param loglevel: Logging level.
    """
    setup_logging(loglevel)
    logger.info("Starting data transformation")

    try:
        logger.debug(f"Loading configuration from {config_file}")
        config_data = {}
        if config_file:
            config_data = load_config(config_file)

        # Apply CLI overrides
        if datasource:
            config_data["datasource"] = {"connection": datasource}
        selector_content = config_data["mapping"]["selector"]
        if selector:
            try:
                with open(selector, "r") as f:
                    selector_content = f.read()
            except (OSError, IOError) as e:
                raise click.ClickException(f"Error reading selector file {selector}: {e}")
        # TODO: simplify this
        config_data["mapping"] = {
            "file": mapping_file or config_data["mapping"]["file"],
            "codelists": codelists_file or config_data["mapping"]["codelists"],
            "ocid_prefix": ocid_prefix or config_data["mapping"]["ocid_prefix"],
            "selector": selector_content,
            "force_publish": force_publish
            if force_publish is not None
            else config_data["mapping"].get("force_publish", False),
        }
        config_data["publishing"] = {
            "publisher": publisher or config_data["publishing"]["publisher"],
            "base_uri": base_uri or config_data["publishing"]["base_uri"],
            "version": version or config_data["publishing"].get("version", ""),
            "publisher_uid": publisher_uid or config_data["publishing"].get("publisher_uid", ""),
            "publisher_scheme": publisher_scheme or config_data["publishing"].get("publisher_scheme", ""),
            "publisher_uri": publisher_uri or config_data["publishing"].get("publisher_uri", ""),
            "extensions": list(extensions) if extensions else config_data["publishing"].get("extensions", []),
        }
        if output_directory:
            config_data["output"] = {"directory": output_directory}

        # Validate final configuration
        config = TypeAdapter(Config).validate_python(config_data)
        mapper = OCDSDataMapper(config)
        writer = DataWriter(config.output)

        ocds_data = mapper.map(DataLoader(config.datasource), validate_mapping=validate_mapping)

        if package:
            logger.info("Packaging data...")
            packer = DataPublisher(config.publishing)
            ocds_data = packer.package(ocds_data)
        logger.info("Writing data...")
        writer.write(ocds_data)
        logger.info("Data transformation completed successfully")

    except Exception as e:
        click.echo(traceback.format_exc())
        raise click.ClickException(f"Error during transformation: {e}")


if __name__ == "__main__":
    run()
