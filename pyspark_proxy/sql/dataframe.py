from pyspark_proxy.proxy import Proxy

__all__ = ['DataFrame']

class DataFrame(Proxy):
    def __init__(self, id):
        self._id = id
