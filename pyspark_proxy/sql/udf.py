import base64
import pickle
import cloudpickle

from pyspark_proxy.proxy import Proxy

class UDFRegistration(Proxy):
    def __init__(self, context_id):
        self._context_id = context_id

    def register(self, name, f, returnType=None):
        pickled_f = base64.b64encode(cloudpickle.dumps(f))

        if returnType != None:
            returnType = {'_PROXY_ID': returnType._id}

        self._call(
                self._context_id,
                'udf.register',
                [(name, {'_CLOUDPICKLE': pickled_f}), {'returnType': returnType}])
