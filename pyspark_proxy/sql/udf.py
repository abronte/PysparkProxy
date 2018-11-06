import base64
import cloudpickle
import functools
import imp
import sys

# workaround to import the python types module which conflicts
# with the pyspark types module
f, pathname, desc = imp.find_module('types', sys.path[1:])
types = imp.load_module('python_types', f, pathname, desc)

from pyspark_proxy.proxy import Proxy

def _copy_func(f):
    g = types.FunctionType(f.func_code, f.func_globals, name=f.func_name,
                           argdefs=f.func_defaults,
                           closure=f.func_closure)
    g = functools.update_wrapper(g, f)
    return g

# From pyspark 2.3 modified to return itself rather than a class
def _wrapped(func):
    assignments = tuple(
        a for a in functools.WRAPPER_ASSIGNMENTS if a != '__name__' and a != '__module__')

    @functools.wraps(func, assigned=assignments)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    wrapper.__name__ = func.__name__
    wrapper.__module__ = (func.__module__ if hasattr(func, '__module__')
                          else func.__class__.__module__)

    wrapper.func = func
    return wrapper

class UDFRegistration(Proxy):
    def __init__(self, context_id):
        self._context_id = context_id

    def register(self, name, f, returnType=None):
        pickled_f = base64.b64encode(cloudpickle.dumps(_copy_func(f)))

        if returnType != None:
            returnType = {'_PROXY_ID': returnType._id}

        self._call(
                self._context_id,
                'udf.register',
                [(name, {'_CLOUDPICKLE': pickled_f}), {'returnType': returnType}])
