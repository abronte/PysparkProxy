from pyspark_proxy.proxy import Proxy

from pyspark_proxy.sql.dataframe import DataFrame

__all__ = ['DataFrameReader']

class DataFrameReader(Proxy):
    def __init__(self, obj, prop):
        self._parent_obj = obj
        self._parent_prop = prop

    def __getattr__(self, name):
        path = '%s.%s' % (self._parent_prop, name)

        def method(*args, **kwargs):
            return self._call(self._parent_obj, path, (args, kwargs))

        return method
