from pyspark_proxy.proxy import Proxy

__all__ = ['Column']

class Column(Proxy):
    def __init__(self, id):
        self._id = id

    def alias(self, *args, **kwargs):
        return self._call(self._id, 'alias', (args, kwargs))

    def cast(self, *args, **kwargs):
        return self._call(self._id, 'cast', (args, kwargs))

    def __repr__(self):
        return self._call(self._id, '__repr__', ((), {}))

    # better way to define these?
    def _op_func(self, name, *args, **kwargs):
        return self._call(self._id, '__neg__', (args, kwargs))

    def __add__(self, *args, **kwargs):
        return self._call(self._id, '__add__', (args, kwargs))

    def __sub__(self, *args, **kwargs):
        return self._call(self._id, '__sub__', (args, kwargs))

    def __mul__(self, *args, **kwargs):
        return self._call(self._id, '__mul__', (args, kwargs))

    def __div__(self, *args, **kwargs):
        return self._call(self._id, '__div__', (args, kwargs))

    def __truediv__(self, *args, **kwargs):
        return self._call(self._id, '__truediv__', (args, kwargs))

    def __mod__(self, *args, **kwargs):
        return self._call(self._id, '__mod__', (args, kwargs))

    def __radd__(self, *args, **kwargs):
        return self._call(self._id, '__radd__', (args, kwargs))

    def __rsub__(self, *args, **kwargs):
        return self._call(self._id, '__rsub__', (args, kwargs))

    def __rmul__(self, *args, **kwargs):
        return self._call(self._id, '__rmul__', (args, kwargs))

    def __rdiv__(self, *args, **kwargs):
        return self._call(self._id, '__rdiv__', (args, kwargs))

    def __rtruediv__(self, *args, **kwargs):
        return self._call(self._id, '__rdiv__', (args, kwargs))

    def __rmod__(self, *args, **kwargs):
        return self._call(self._id, '__rmod__', (args, kwargs))

    def __pow__(self, *args, **kwargs):
        return self._call(self._id, '__pow__', (args, kwargs))

    def __rpow__(self, *args, **kwargs):
        return self._call(self._id, '__rpow__', (args, kwargs))

    def __eq__(self, *args, **kwargs):
        return self._call(self._id, '__eq__', (args, kwargs))

    def __ne__(self, *args, **kwargs):
        return self._call(self._id, '__ne__', (args, kwargs))

    def __lt__(self, *args, **kwargs):
        return self._call(self._id, '__lt__', (args, kwargs))

    def __le__(self, *args, **kwargs):
        return self._call(self._id, '__le__', (args, kwargs))

    def __ge__(self, *args, **kwargs):
        return self._call(self._id, '__ge__', (args, kwargs))

    def __gt__(self, *args, **kwargs):
        return self._call(self._id, '__gt__', (args, kwargs))

    def __and__(self, *args, **kwargs):
        return self._call(self._id, '__and__', (args, kwargs))

    def __or__(self, *args, **kwargs):
        return self._call(self._id, '__or__', (args, kwargs))

    def __invert__(self, *args, **kwargs):
        return self._call(self._id, '__invert__', (args, kwargs))

    def __rand__(self, *args, **kwargs):
        return self._call(self._id, '__rand__', (args, kwargs))

    def __ror__(self, *args, **kwargs):
        return self._call(self._id, '__ror__', (args, kwargs))
