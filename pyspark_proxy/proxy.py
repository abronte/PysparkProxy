import sys
import uuid
import json

import requests

class Proxy(object):
    _PROXY = True
    _id = None
    _args = None
    _kwargs = None
    _module = None
    _class = None

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

        print('\ncreate object:')
        print(body)

        r = requests.post('http://localhost:5000/create', json=body)
        print(r.status_code)

    def _call(self, base_obj, path, function_args):
        print('\n_call %s on %s' % (path, base_obj))

        body = {
                'id': base_obj,
                'path': path,
                'args': function_args[0],
                'kwargs': function_args[1]
                }

        print(body)

        r = requests.post('http://localhost:5000/call', json=body)
        print(r.status_code)

        res_json = r.json()
        print(res_json)
        
        if res_json['stdout'] != []:
            print('\n'.join(res_json['stdout']))

        if res_json['object']:
            if 'id' in res_json:

                if res_json['class'] == 'DataFrame':
                    print('created dataframe object')
                    from pyspark_proxy.sql.dataframe import DataFrame

                    return DataFrame(res_json['id'])
            else:
                return res_json['value']
        else:
            return None

    def __getattr__(self, name):
        def method(*args, **kwargs):
            return self._call(self._id, name, (args, kwargs))

        print('%s.%s' % (self._class, name))

        return method
