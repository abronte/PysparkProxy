from pyspark_proxy.proxy import Proxy

__all__ = ['DataFrameReader', 'DataFrameWriter']

class DataFrameReader(Proxy):
    _chain_functions = ['option', 'options', 'format', 'schema']

    def __init__(self, obj, prop):
        self._parent_obj = obj
        self._parent_prop = prop

        self._func_chain = [{'func': self._parent_prop}]

    def __getattr__(self, name):
        def method(*args, **kwargs):
            self._func_chain.append({'func': name, 'args': args, 'kwargs': kwargs})

            if name in self._chain_functions:
                return self
            else:
                return self._call_chain(self._parent_obj)

        return method

class DataFrameWriter(Proxy):
    _chain_functions = [
            'format', 'mode', 'option', 'options',
            'partitionBy', 'bucketBy', 'sortBy']

    def __init__(self, obj, prop):
        self._parent_obj = obj
        self._parent_prop = prop

        self._func_chain = [{'func': self._parent_prop}]

    def __getattr__(self, name):
        def method(*args, **kwargs):
            self._func_chain.append({'func': name, 'args': args, 'kwargs': kwargs})

            if name in self._chain_functions:
                return self
            else:
                return self._call_chain(self._parent_obj)

        return method
