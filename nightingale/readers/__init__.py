from .csv import CSVOcdsReader # noqa
from .json import JSONOcdsReader # noqa


READERS = {
    'csv': CSVOcdsReader,
    'json': JSONOcdsReader
}
