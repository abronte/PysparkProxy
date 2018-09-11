import sys
import uuid
import json
import pickle
import base64

import requests

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

        self._create_object()

    def _create_object(self):
        args = []

        for x in self._args:
            if hasattr(x, '_PROXY'):
                args.append({'_PROXY_ID': x._id})
            else:
                args.append(x)

        body = {
                'module': self._module,
                'class': self._class,
                'kwargs': self._kwargs,
                'args': args,
                'id': self._id
                }

        r = requests.post('http://localhost:5000/create', json=body)

    # for a single function call
    # ex: df.write.csv('foo.csv')
    # ex: df.count()
    def _call(self, base_obj, path, function_args):
        args = []

        for x in function_args[0]:
            if hasattr(x, '_PROXY'):
                args.append({'_PROXY_ID': x._id})
            else:
                args.append(x)

        body = {
                'id': base_obj,
                'path': path,
                'args': args,
                'kwargs': function_args[1]
                }

        r = requests.post('http://localhost:5000/call', json=body)
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

        r = requests.post('http://localhost:5000/call_chain', json=body)
        res_json = r.json()

        self._func_chain = []

        return self._handle_response(res_json)

    # __getitem__ server call 
    # ex: df['age']
    def _get_item(self, item):
        body = {
            'id': self._id,
            'item': item
            }

        r = requests.get('http://localhost:5000/get_item', json=body)
        return r.json()

    # parses the response json from the server and returns the proper object
    def _handle_response(self, resp):
        if resp['stdout'] != []:
            print('\n'.join(resp['stdout']))

        if resp['object']:
            if 'id' in resp:
                if resp['class'] == 'DataFrame':
                    from pyspark_proxy.sql.dataframe import DataFrame

                    return DataFrame(resp['id'])
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
