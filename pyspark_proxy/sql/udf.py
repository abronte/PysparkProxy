import base64
import imp
import sys

from pyspark_proxy.proxy import Proxy
from pyspark_proxy.sql.types import DataType
from pyspark_proxy.sql.column import Column

class UDFRegistration(Proxy):
    def __init__(self, context_id):
        self._context_id = context_id

    def register(self, name, f, returnType=None):
        if returnType != None:
            returnType = {'_PROXY_ID': returnType._id}

        self._call(
                self._context_id,
                'udf.register',
                [(name, f), {'returnType': returnType}])

class UserDefinedFunction(Proxy):
    def __init__(self, f, returnType=None):
        if isinstance(returnType, DataType):
            returnType = {'_PROXY_ID': returnType._id}

        result = self._call(
                'pyspark',
                'sql.functions.udf',
                [(f, returnType), {}])

        self._id = result['id']

    def __call__(self, *args, **kwargs):
        result = self._call(
                self._id,
                None,
                [args, kwargs])

        result._name = args[0]

        return result
