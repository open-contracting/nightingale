import click
import click_pathlib
from .mapper import OCDSDataMapper
from .loader import DataLoader, PDLoader
from .writer import DataWriter
from .mapping.v1 import Mapping
from .publisher import DataPublisher
from .config import Config


@click.command()
@click.option(
    '--config',
    'config_file',
    help='Path to the configuration file',
    type=click_pathlib.Path(exists=True),
    required=True,
)
@click.option('--package', is_flag=True, default=False, help='Package data')
def run(config_file, package):
    click.echo('Start transforming')
    config = Config.from_file(config_file)
    mapper = OCDSDataMapper(config)
    writer = DataWriter(config.output)
    ocds_data = mapper.map()
    if package:
        # XXX: this step should be optional as ocdskit can package data
        packer = DataPublisher(config.publishing)
        ocds_data = packer.package(ocds_data)
        writer.write(ocds_data)

    else:
        writer.write({"releases": ocds_data})
