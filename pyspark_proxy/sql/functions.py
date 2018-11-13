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
    def _(*args, **kwargs):
        return Function(name, *args, **kwargs).obj

    return _

_functions = [
    # regular functions that return a column
    # these are defined in an array in pyspark
    # from pyspark, 1.3, 1.4, 1.6, 2.1 and string functions
    'lit', 'col', 'column', 'asc', 'desc', 'upper', 'lower'
    'sqrt', 'abs', 'max', 'min', 'count', 'sum', 'avg', 'mean',
    'sumDistinct', 'acos', 'asin', 'atan', 'cbrt', 'ceil', 'cos',
    'cosh', 'exp', 'expm1', 'floor', 'log', 'log10', 'log1p',
    'rint', 'signum', 'sin', 'sinh', 'tan', 'tanh', 'bitwiseNOT',
    'stddev', 'stddev_samp', 'stddev_pop', 'variance', 'var_samp',
    'var_pop', 'skewness', 'kurtosis', 'collect_list',
    'collect_set', 'degrees', 'radians',
    'ascii', 'base64', 'unbase64', 'initcap', 'lower', 'upper',
    'reverse', 'ltrim', 'rtrim', 'trim',
    # functions with individually implemented in pyspark
    'approx_count_distinct', 'broadcast', 'coalsece', 'corr',
    'covar_pop', 'covar_samp', 'countDistinct', 'first',
    'grouping', 'grouping_id', 'input_file_name', 'isnan',
    'isnull', 'last', 'monotonically_increasing_id', 'nanvl',
    'rand', 'randn', 'round', 'bround', 'shiftLeft',
    'shiftRight', 'shiftRightUnsigned', 'spark_parition_id',
    'expr', 'struct', 'greatest', 'least', 'when', 'log',
    'log2', 'conv', 'factorial', 'lag', 'lead', 'ntile',
    'current_date', 'current_timestamp', 'date_format',
    'year', 'quarter', 'month', 'dayofweek', 'dayofmonth',
    'dayofyear', 'hour', 'minute', 'second', 'weekofyear',
    'date_add', 'date_sub', 'datediff', 'add_months',
    'months_between', 'to_date', 'to_timestamp', 'trunc',
    'date_trunc', 'next_day', 'last_day', 'from_unixtime',
    'unix_timestamp', 'from_utc_timestamp', 'to_utc_timestamp',
    'window', 'crc32', 'md5', 'sha1', 'sha2', 'hash',
    'concat', 'concat_ws', 'decode', 'encode', 'format_number',
    'format_string', 'instr', 'substring', 'substring_index',
    'levenshtein', 'locate', 'lpad', 'rpad', 'repeat',
    'split', 'regexp_extract', 'regexp_replace', 'initcap',
    'soundex', 'bin', 'hex', 'unhex', 'length', 'translate',
    'create_map', 'array', 'array_contains', 'explode',
    'posexplode', 'explode_outer', 'posexplode_outer',
    'get_json_object', 'json_tuple', 'from_json', 'to_json',
    'size', 'sort_array', 'map_keys', 'map_values'
    ]

for _name in _functions:
    globals()[_name] = _create_function(_name)

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
