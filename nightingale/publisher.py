from datetime import datetime
from typing import Any


class DataPublisher:
    """Packs array of releases into a release package"""

    def __init__(self, config):
        self.config = config

    def produce_ocid(self, value):
        prefix = self.config.ocid_prefix
        return f'{prefix}-{value}'

    def publish(self, data: list[dict[str, Any]]):
        now = datetime.now().isoformat()
        for r in data:
            r['ocid'] = self.produce_ocid(r['ocid'])
        # XXX: should it be now or other date?
        return {
            'uri': f'https://todo.com/{now}',
            'version': self.config.version,
            'publisher': self.config.publisher,
            'publishedDate': now,
            'releases': data
        }
