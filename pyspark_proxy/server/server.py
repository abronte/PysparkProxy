import uuid
import os
import pickle
import base64

import pyspark
from flask import Flask, request, jsonify

from pyspark_proxy.server.capture import Capture

app = Flask(__name__)

objects = {}

@app.route('/create', methods=['POST'])
def create():
    global objects
    
    req = request.json

    print('\nCREATE OBJECT')
    print(req)

    module_paths = req['module'].split('.')
    base_module = __import__(module_paths[0])

    module = base_module

    # __import__ only imports the top level module
    # use loop through the rest via getattr to get the 
    # last module with the needed object
    if len(module_paths) > 1:
        for m in module_paths[1:]:
            module = getattr(module, m)

    callable = getattr(module, req['class'])

    args = []

    if len(req['args']) > 0:
        for a in req['args']:
            if type(a) == dict and '_PROXY_ID' in a:
                id = a['_PROXY_ID']
                print('Retrieving object id: %s' % id)

                args.append(objects[id])

        objects[req['id']] = callable(*tuple(args), **req['kwargs'])
    else:
        objects[req['id']] = callable(**req['kwargs'])

    print(objects)

    return req['id']

@app.route('/call', methods=['POST'])
def call():
    global objects
    
    req = request.json

    print('\nCALL METHOD')
    print(req)

    base_obj = objects[req['id']]
    paths = req['path'].split('.') 

    func = base_obj
    
    for p in paths:
        func = getattr(func, p)

    with Capture() as stdout:
        res_obj = func(*req['args'], **req['kwargs'])

    result = {
            'object': False,
            'stdout': stdout
            }

    if res_obj is not None:
        result['object'] = True
        result['class'] = res_obj.__class__.__name__

        if 'pyspark' in str(res_obj.__class__):
            id = str(uuid.uuid4())

            result['id'] = id

            print('Adding object id %s to the stack' % id)
            objects[id] = res_obj
        elif 'pandas' in str(res_obj.__class__):
            result['class'] = str(res_obj.__class__)
            result['value'] = base64.b64encode(pickle.dumps(res_obj, 2))
        else:
            result['value'] = res_obj

    return jsonify(result)

@app.route('/call_chain', methods=['POST'])
def call_chain():
    global object

    req = request.json

    print('\nSERVER: CALL CHAIN')
    print(req)

    base_obj = objects[req['id']]

    obj = base_obj

    with Capture() as stdout:
        for s in req['stack']:
            obj = getattr(obj, s['func'])

            if callable(obj):
                obj = obj(*s['args'], **s['kwargs'])

    result = {
            'object': False,
            'stdout': stdout
            }

    return jsonify(result)

@app.route('/clear', methods=['POST', 'GET'])
def clear():
    global objects

    for o in objects:
        obj = objects[o]

        if type(obj) == pyspark.SparkContext:
            obj.stop()

    objects = {}

    return 'ok'

def run(*args, **kwargs):
    app.run(*args, **kwargs)


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
