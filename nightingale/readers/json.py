import simplejson as json

class JSONOcdsReader:
    def __init__(self, path):
        self.path = path

    def iter_rows(self):
        with open(self.path) as f:
            data = json.load(f)
            # XXX
            for row in data:
                yield row
