from pyspark_proxy.proxy import Proxy

class GroupedData(Proxy):
    def __init__(self, id):
        self._id = id
