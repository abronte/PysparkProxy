import functools

from pyspark_proxy.sql.types import DataType
from pyspark_proxy.sql.udf import UserDefinedFunction

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
