import os
import sys
import uuid
import json
import pickle
import base64
import types
import functools

import cloudpickle
import requests

PROXY_URL = os.environ.get('PYSPARK_PROXY_URL', 'http://127.0.0.1:8765')

def _copy_func(f):
    g = types.FunctionType(f.func_code, f.func_globals, name=f.func_name,
                           argdefs=f.func_defaults,
                           closure=f.func_closure)
    g = functools.update_wrapper(g, f)
    return g

class Proxy(object):
    _PROXY = True
    _id = None
    _args = None
    _kwargs = None
    _module = None
    _class = None
    _func_chain = []

    def __init__(self, *args, **kwargs):
        self._id = str(uuid.uuid4())
        self._kwargs = kwargs
        self._class = self.__class__.__name__
        self._args = args
        self._module = sys.modules[self.__class__.__module__].__name__.replace('pyspark_proxy', 'pyspark')

        if 'no_init' not in kwargs:
            self._create_object()

        # pickled objects returned from the server can have the pyspark path
        # module instead of pyspark_proxy module. this creates an alias for
        # pyspark to point to pyspark_proxy so hopefully pickleable objects will
        # use the pyspark_proxy version of the object.
        if 'pyspark' not in sys.modules:
            sys.modules['pyspark'] = sys.modules['pyspark_proxy']

    def _create_object(self):
        args, kwargs = self._prepare_args(self._args, self._kwargs)

        body = {
                'module': self._module,
                'class': self._class,
                'kwargs': kwargs,
                'args': args,
                'id': self._id
                }

        r = requests.post(PROXY_URL+'/create', json=body)

    # for a single function call
    # ex: df.write.csv('foo.csv')
    # ex: df.count()
    def _call(self, base_obj, path, function_args):
        args, kwargs = self._prepare_args(function_args[0], function_args[1])

        body = {
                'id': base_obj,
                'path': path,
                'args': args,
                'kwargs': kwargs
                }

        r = requests.post(PROXY_URL+'/call', json=body)
        res_json = r.json()

        return self._handle_response(res_json)

    # for chained function calls
    # ex: df.format('json').save('bar.json')
    # 
    # currently any implemented function doesn't return an object
    # if in a later time _call_chain needs to return an object, might
    # merge this with the regular _call function
    def _call_chain(self, base_obj):
        body = {
            'id': base_obj,
            'stack': self._func_chain
            }

        r = requests.post(PROXY_URL+'/call_chain', json=body)
        res_json = r.json()

        self._func_chain = []

        return self._handle_response(res_json)

    # for class function calls
    # ex: SQLContext.getOrCreate(spark_context)
    #
    # This might need to be reworked once more class methods are
    # implemented and there is a better understanding of requirements
    @classmethod
    def _call_class_method(cls, function, function_args):
        args = []

        for x in function_args[0]:
            if hasattr(x, '_PROXY'):
                args.append({'_PROXY_ID': x._id})
            else:
                args.append(x)

        body = {
            'class': cls.__name__,
            'module': cls.__module__.replace('pyspark_proxy', 'pyspark'),
            'function': function,
            'args': args,
            'kwargs': function_args[1]
        }

        r = requests.post(PROXY_URL+'/call_class_method', json=body)
        res_json = r.json()

        return res_json

    # __getitem__ server call 
    # ex: df['age']
    def _get_item(self, item):
        body = {
            'id': self._id,
            'item': item
            }

        r = requests.get(PROXY_URL+'/get_item', json=body)
        return r.json()

    # parses the response json from the server and returns the proper object
    def _handle_response(self, resp):
        if resp['stdout'] != []:
            print('\n'.join(resp['stdout']))

        if resp['exception']:
            raise Exception(resp['exception'])

        if resp['object']:
            if 'id' in resp:
                # this could be improved
                if resp['class'] == 'DataFrame':
                    from pyspark_proxy.sql.dataframe import DataFrame

                    return DataFrame(resp['id'])
                elif resp['class'] == 'Column':
                    from pyspark_proxy.sql.column import Column

                    return Column(resp['id'])
                elif resp['class'] == 'GroupedData':
                    from pyspark_proxy.sql.group import GroupedData

                    return GroupedData(resp['id'])
                elif resp['class'] == 'RDD':
                    from pyspark_proxy.rdd import RDD

                    return RDD(resp['id'])
                elif resp['class'] == 'PipelinedRDD':
                    from pyspark_proxy.rdd import RDD

                    return RDD(resp['id'])
                else:
                    return resp
            elif 'pickle' == resp['class']:
                return pickle.loads(base64.b64decode(resp['value']))
            else:
                return resp['value']
        else:
            return None

    # catch all function
    def __getattr__(self, name):
        def method(*args, **kwargs):
            return self._call(self._id, name, (args, kwargs))

        return method

    def __repr__(self):
        return self._call(self._id, '__repr__', ((), {}))

    # since we can't send actual objects over to the server, we 
    # need to replace any pyspark related objects passed in functions
    # with a placeholder so the actual pyspark object on the server
    # gets passed in with the function call
    def _prepare_args(self, args, kwargs):
        prepared_args = []
        prepared_kwargs = {}

        for a in args:
            arg_type = type(a)

            # pyspark objects can sometimes be in lists so we need to
            # check the list and send their id over so the server knows
            # what to retrieve
            if arg_type == list:
                processed_list = []

                for x in a:
                    processed_list.append(self._proxy_obj_replace(x))

                prepared_args.append(processed_list)
            elif arg_type == types.FunctionType:
                pickled_f = base64.b64encode(cloudpickle.dumps(_copy_func(a)))
                prepared_args.append({'_CLOUDPICKLE': pickled_f})
            else:
                prepared_args.append(self._proxy_obj_replace(a))

        for a in kwargs:
            v = kwargs[a]
            prepared_kwargs[a] = self._proxy_obj_replace(v)

        return prepared_args, prepared_kwargs

    def _proxy_obj_replace(self, obj):
        if hasattr(obj, '_PROXY'):
            return {'_PROXY_ID': obj._id}
        else:
            return obj
