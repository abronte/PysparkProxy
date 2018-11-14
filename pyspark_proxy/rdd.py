from pyspark_proxy.proxy import Proxy

__all__ = ["RDD"]

class RDD(Proxy):
    def __init__(self, id):
        self._id = id

    def __repr__(self):
        return self._call(self._id, '__repr__', ((), {}))

    def __add__(self, *args, **kwargs):
        return self._call(self._id, '__add__', (args, kwargs))

class PipelinedRDD(RDD):
    pass
