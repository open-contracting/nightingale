import logging
import tomllib
import traceback

import click
import click_pathlib
from pydantic import TypeAdapter

from nightingale.config import Config
from nightingale.loader import DataLoader
from nightingale.mapper import OCDSDataMapper
from nightingale.publisher import DataPublisher
from nightingale.writer import DataWriter

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
@click.option("--stream/--no-stream", default=True, help="Enable streaming to output file")
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
    stream,
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

    writer = None
    try:
        logger.debug(f"Loading configuration from {config_file}")
        config_data = {}
        if config_file:
            config_data = load_config(config_file)

        # Apply CLI overrides
        if datasource:
            config_data["datasource"] = {"connection": datasource}
        if selector:
            try:
                with open(selector) as f:
                    config_data["mapping"]["selector"] = f.read()
            except OSError as e:
                raise click.ClickException(f"Error reading selector file {selector}: {e}")
        if mapping_file:
            config_data["mapping"]["file"] = mapping_file
        if codelists_file:
            config_data["mapping"]["codelists"] = codelists_file
        if ocid_prefix:
            config_data["mapping"]["ocid_prefix"] = ocid_prefix
        if force_publish:
            config_data["mapping"]["force_publish"] = force_publish
        if publisher:
            config_data["publishing"]["publisher"] = publisher
        if base_uri:
            config_data["publishing"]["base_uri"] = base_uri
        if version:
            config_data["publishing"]["version"] = version
        if publisher_uid:
            config_data["publishing"]["publisher_uid"] = publisher_uid
        if publisher_scheme:
            config_data["publishing"]["publisher_scheme"] = publisher_scheme
        if publisher_uri:
            config_data["publishing"]["publisher_uri"] = publisher_uri
        if extensions:
            config_data["publishing"]["extensions"] = list(extensions)
        if output_directory:
            config_data["output"] = {"directory": output_directory}

        # Validate final configuration
        config = TypeAdapter(Config).validate_python(config_data)
        writer = DataWriter(config.output)

        if stream:
            logger.info("Starting stream-based processing...")

            # Create mapper first to get access to the mapping template
            mapper = OCDSDataMapper(config, writer=writer)

            # Now create packer with the loaded mapping
            packer = DataPublisher(config.publishing, mapper.mapping)
            package_metadata = packer.package([])
            del package_metadata["releases"]

            writer.start_package_stream(package_metadata)

            # The mapper already has the writer instance, so we can just call map
            mapper.map(DataLoader(config.datasource), validate_mapping=validate_mapping)
            logger.info("Streaming data completed.")

        else:
            logger.info("Starting in-memory processing...")
            mapper = OCDSDataMapper(config)
            ocds_data = mapper.map(DataLoader(config.datasource), validate_mapping=validate_mapping)

            if package:
                logger.info("Packaging data...")
                packer = DataPublisher(config.publishing, mapper.mapping)
                ocds_data = packer.package(ocds_data)

            logger.info("Writing data...")
            writer.write(ocds_data)

        logger.info("Data transformation completed successfully")

    except Exception as e:
        click.echo(traceback.format_exc())
        raise click.ClickException(f"Error during transformation: {e}")
    finally:
        if writer and writer.is_streaming():
            logger.info("Finalizing stream file...")
            writer.end_package_stream()


if __name__ == "__main__":
    run()
