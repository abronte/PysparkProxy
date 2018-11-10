import functools

from pyspark_proxy.proxy import Proxy

from pyspark_proxy.sql.types import DataType
from pyspark_proxy.sql.udf import UserDefinedFunction
from pyspark_proxy.sql.column import Column

class Function(Proxy):
    obj = None

    def __init__(self, name, *args, **kwargs):
        result = self._call(
                'pyspark',
                'sql.functions.%s' % name,
                [args, kwargs])

        self.obj = result


def _create_function(name):
    def _(col):
        return Function(name, col).obj

    return _

# regular functions that return a column
# from pyspark, 1.3, 1.4, 1.6, 2.1 functions
_functions = [
    'lit', 'col', 'column', 'asc', 'desc', 'upper', 'lower'
    'sqrt', 'abs', 'max', 'min', 'count', 'sum', 'avg', 'mean',
    'sumDistinct', 'acos', 'asin', 'atan', 'cbrt', 'ceil', 'cos',
    'cosh', 'exp', 'expm1', 'floor', 'log', 'log10', 'log1p',
    'rint', 'signum', 'sin', 'sinh', 'tan', 'tanh', 'bitwiseNOT',
    'stddev', 'stddev_samp', 'stddev_pop', 'variance', 'var_samp',
    'var_pop', 'skewness', 'kurtosis', 'collect_list',
    'collect_set', 'degrees', 'radians'
    ]

for _name in _functions:
    globals()[_name] = _create_function(_name)

def approx_count_distinct(col, rsd=None):
    return Function('approx_count_distinct', col, rsd).obj

def broadcast(df):
    return Function('broadcast', df).obj

# in the pyspark code, returnType defaults to the
# StringType() datatype but for some reason it errors
# in pyspark proxy
def udf(f=None, returnType='string'):
    if f is None or isinstance(f, (str, DataType)):
        return_type = f or returnType
        return functools.partial(UserDefinedFunction, returnType=return_type)
    else:
        return UserDefinedFunction(f, returnType)

blacklist = ['map', 'since', 'ignore_unicode_prefix']
__all__ = [k for k, v in globals().items()
           if not k.startswith('_') and k[0].islower() and callable(v) and k not in blacklist]
__all__.sort()
