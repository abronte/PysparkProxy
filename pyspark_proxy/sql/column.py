from pyspark_proxy.proxy import Proxy

__all__ = ['Column']

class Column(Proxy):
    def __init__(self, id, dfid, name):
        self._dfid = dfid
        self._name = name
        self._id = id

    def alias(self, *args, **kwargs):
        res = self._call(self._id, 'alias', (args, kwargs))

        return Column(res['id'], self._dfid, args[0])

    def cast(self, *args, **kwargs):
        res = self._call(self._id, 'cast', (args, kwargs))

        return Column(res['id'], self._dfid, self._name)
