from datetime import datetime
from pathlib import Path
import simplejson as json


def new_name(package):
    date = package.get('publishedDate', datetime.now().isoformat() )
    return f'release-package-{date}.json'


class DataWriter:
    """Writes release package to disk"""

    def __init__(self, config):
        self.config = config

    def make_dirs(self):
        base = Path(self.config.directory)
        base.mkdir(parents=True, exist_ok=True)
        return base

    def get_output_path(self, package):
        base = self.make_dirs()
        return base / new_name(package)

    def write(self, package):
        path = self.get_output_path(package)
        with path.open('w') as f:
            json.dump(package, f, indent=2)
