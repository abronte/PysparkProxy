from pyspark_proxy.proxy import Proxy

__all__ = ['Column']

class Column(Proxy):
    def __init__(self, id, name=None):
        self._name = name
        self._id = id

    def alias(self, *args, **kwargs):
        res = self._call(self._id, 'alias', (args, kwargs))
        res._name = args[0]

        return res

    def cast(self, *args, **kwargs):
        res = self._call(self._id, 'cast', (args, kwargs))
        res._name = self._name

        return res

    def __repr__(self):
        return 'Column<%s>' % self._name.encode('utf8')
