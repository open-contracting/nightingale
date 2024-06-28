import logging

import click
import click_pathlib

from .config import Config
from .loader import DataLoader
from .mapper import OCDSDataMapper
from .publisher import DataPublisher
from .writer import DataWriter

# Set up logging
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
def run(config_file, package, validate_mapping, loglevel):
    setup_logging(loglevel)
    logger.info("Start transforming")

    try:
        config = Config.from_file(config_file)
        mapper = OCDSDataMapper(config)
        writer = DataWriter(config.output)
        logger.info("Mapping data...")
        ocds_data = mapper.map(DataLoader(config.datasource), validate_mapping=validate_mapping)

        if package:
            logger.info("Packaging data...")
            packer = DataPublisher(config.publishing)
            ocds_data = packer.package(ocds_data)

        logger.info("Writing data...")
        writer.write({"releases": ocds_data} if not package else ocds_data)
        logger.info("Data transformation completed successfully")

    except Exception as e:
        raise click.ClickException(f"Error during transformation: {e}")


if __name__ == "__main__":
    run()
