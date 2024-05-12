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
def run(config_file):
    click.echo('Start transforming')
    config = Config.from_file(config_file)
    mapping = Mapping(config.mapping)
    mapper = OCDSDataMapper(mapping)
    writer = DataWriter(config.output)
    packer = DataPublisher(config.publishing)

    ocds_data = mapper.map(load_data(config, mapping))
    package = packer.publish(ocds_data)
    writer.write(package)


def load_data(config, mapping):
    # loader = PDLoader(config.datasources)
    # loader.load()
    # data = loader.get_data()
    loader = DataLoader(config.datasources)
    loader.load()
    # TODO: validate in separate class
    loader.validate_data_elements(mapping.data_elements)
    loader.validate_selector(mapping.data_elements)
    data = loader.get_data()
    return data
