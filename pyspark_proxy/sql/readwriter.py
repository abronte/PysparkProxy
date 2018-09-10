from pyspark_proxy.proxy import Proxy

__all__ = ['DataFrameReader', 'DataFrameWriter']

class DataFrameReader(Proxy):
    def __init__(self, obj, prop):
        self._parent_obj = obj
        self._parent_prop = prop

    def option(self, *args, **kwargs):
        self._func_chain.append({'func': 'option', 'args': args, 'kwargs': kwargs})

        return self

    def options(self, *args, **kwargs):
        self._func_chain.append({'func': 'options', 'args': args, 'kwargs': kwargs})

        return self

    def format(self, *args, **kwargs):
        self._func_chain.append({'func': 'format', 'args': args, 'kwargs': kwargs})

        return self

    def schema(self, *args, **kwargs):
        self._func_chain.append({'func': 'schema', 'args': args, 'kwargs': kwargs})

        return self

    def __getattr__(self, name):
        self._func_chain.insert(0, {'func': self._parent_prop})

        def method(*args, **kwargs):
            self._func_chain.append({'func': name, 'args': args, 'kwargs': kwargs})

            return self._call_chain(self._parent_obj)

        return method

class DataFrameWriter(Proxy):
    def __init__(self, obj, prop):
        self._parent_obj = obj
        self._parent_prop = prop

    def format(self, *args, **kwargs):
        self._func_chain.append({'func': 'format', 'args': args, 'kwargs': kwargs})

        return self

    def mode(self, *args, **kwargs):
        self._func_chain.append({'func': 'mode', 'args': args, 'kwargs': kwargs})

        return self

    def option(self, *args, **kwargs):
        self._func_chain.append({'func': 'option', 'args': args, 'kwargs': kwargs})

        return self

    def options(self, *args, **kwargs):
        self._func_chain.append({'func': 'options', 'args': args, 'kwargs': kwargs})

        return self

    def __getattr__(self, name):
        self._func_chain.insert(0, {'func': self._parent_prop})

        def method(*args, **kwargs):
            self._func_chain.append({'func': name, 'args': args, 'kwargs': kwargs})

            return self._call_chain(self._parent_obj)

        return method
