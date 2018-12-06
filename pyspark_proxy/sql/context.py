from pyspark_proxy.proxy import Proxy

from pyspark_proxy.sql.readwriter import DataFrameReader

__all__ = ['SQLContext']

class SQLContext(Proxy):
    @classmethod
    def getOrCreate(cls, *args, **kwargs):
        res = cls._call_class_method('getOrCreate', (args, kwargs))

        return SQLContext(_id=res['id'])

    @property
    def read(self):
        return DataFrameReader(self._id, 'read')

    @property
    def udf(self):
        from pyspark_proxy.sql.udf import UDFRegistration
        return UDFRegistration(self._id)
