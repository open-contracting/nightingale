
from .loader import DataLoader

def load_data(datasource, selector):
    # loader = PDLoader(config.datasources)
    loader = DataLoader(datasource)
    return loader.load(selector)
    # TODO: validate in separate class
    #loader.validate_data_elements(mapping.data_elements)
    #loader.validate_selector(mapping.data_elements)
