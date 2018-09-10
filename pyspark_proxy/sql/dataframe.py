from pyspark_proxy.proxy import Proxy

from pyspark_proxy.sql.readwriter import DataFrameWriter
from pyspark_proxy.sql.column import Column

__all__ = ['DataFrame']

class DataFrame(Proxy):
    _column_obj = {}

    def __init__(self, id):
        self._id = id

    @property
    def dtypes(self):
        return self._call(self._id, 'dtypes', ((), {}))

    @property
    def columns(self):
        return self._call(self._id, 'columns', ((), {}))

    @property
    def write(self):
        return DataFrameWriter(self._id, 'write')

    # retrieves column by key lookup
    # df['age']
    def __getitem__(self, item):
        if item not in self._column_obj:
            res = self._get_item(item)
            col = Column(res['id'], self._id, item)
        else:
            col = self._column_obj[item]

        return col

