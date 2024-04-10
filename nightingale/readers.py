import openpyxl


class XlsxReader:

    def __init__(self, path) -> None:
        self.path = path

    def read(self):
        return openpyxl.load_workbook(self.path)

    def _dictify(self, reader, key):
        data = {}
        for row in reader:
            try:
                data[row[key]] = row
            except KeyError:
                import pdb; pdb.set_trace()
        return data
