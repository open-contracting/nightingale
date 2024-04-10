import csv


class CSVOcdsReader:

    def __init__(self, path) -> None:
        self.path = path

    def iter_rows(self):
        with open(self.path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row
