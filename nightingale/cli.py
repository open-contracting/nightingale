import click
import click_pathlib
from .mapper import OCDSDataMapper, MapperConfig


@click.command()
@click.option(
    '--config',
    'config_file',
    help='Path to the configuration file',
    type=click_pathlib.Path(exists=True),
    required=True,
)
def run(config_file):
    conf = MapperConfig(config_file)
    tr = OCDSDataMapper(conf)
    click.echo('Start transforming')
    tr.transform()
