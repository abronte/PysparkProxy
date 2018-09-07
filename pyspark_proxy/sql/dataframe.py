from pyspark_proxy.proxy import Proxy

from pyspark_proxy.sql.readwriter import DataFrameWriter

__all__ = ['DataFrame']

class DataFrame(Proxy):
    dfw = None

    def __init__(self, id):
        self._id = id

    @property
    def write(self):
        if self.dfw == None:
            self.dfw = DataFrameWriter(self._id, 'write')

        return self.dfw
