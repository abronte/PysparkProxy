from pyspark_proxy.proxy import Proxy

from pyspark_proxy.sql.readwriter import DataFrameReader

__all__ = ['SQLContext']

class SQLContext(Proxy):
    @property
    def read(self):
        return DataFrameReader(self._id, 'read')
