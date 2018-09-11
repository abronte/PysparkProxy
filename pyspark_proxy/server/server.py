import uuid
import os
import pickle
import base64

import pyspark
from flask import Flask, request, jsonify

from pyspark_proxy.server.capture import Capture

app = Flask(__name__)

objects = {}

# looks at incomming arguments to see if there are any
# pyspark objects that should be retrieved and passed in
def arg_objects(request_args):
    global objects

    args = []

    for a in request_args:
        if type(a) == dict and '_PROXY_ID' in a:
            id = a['_PROXY_ID']
            print('Retrieving object id: %s' % id)

            args.append(objects[id])
        else:
            args.append(a)

    return args

# checks if any objects are created via a function call
# and returns the corresponding object
#
# pandas: pickles and returns a pandas dataframe
# pyspark.*: returns class and corresponding object id
def object_response(obj, exception, paths=[], stdout=[]):
    global objects

    result = {
            'object': False,
            'stdout': stdout,
            'exception': exception
            }

    if obj is not None:
        result['object'] = True
        result['class'] = obj.__class__.__name__

        if 'pyspark' in str(obj.__class__):
            id = str(uuid.uuid4())

            result['id'] = id

            print('Adding object id %s to the stack' % id)
            objects[id] = obj
        # the last string in paths is the function that gets called
        elif paths[-1] == 'toPandas' or paths[-1] == 'collect':
            result['class'] = 'pickle'
            result['value'] = base64.b64encode(pickle.dumps(obj, 2))
        else:
            result['value'] = obj

    return result

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
    objects[req['id']] = callable(*arg_objects(req['args']), **req['kwargs'])

    return req['id']

@app.route('/call', methods=['POST'])
def call():
    global objects
    
    req = request.json

    print('\nCALL METHOD')
    print(req)

    result_exception = None
    base_obj = objects[req['id']]
    paths = req['path'].split('.')

    func = base_obj

    try:
        for p in paths:
            func = getattr(func, p)
        
        with Capture() as stdout:

            if callable(func):
                res_obj = func(*arg_objects(req['args']), **req['kwargs'])
            else:
                res_obj = func
    except Exception as e:
        result_exception = str(e)

    return jsonify(object_response(res_obj, result_exception, paths, stdout))

@app.route('/call_chain', methods=['POST'])
def call_chain():
    global object

    req = request.json

    print('\nSERVER: CALL CHAIN')
    print(req)

    base_obj = objects[req['id']]

    obj = base_obj
    res_obj = None
    result_exception = None

    try:
        with Capture() as stdout:
            for s in req['stack']:
                obj = getattr(obj, s['func'])

                if callable(obj):
                    obj = obj(*s['args'], **s['kwargs'])

                    # only check for returned objects for the
                    # last call in the chain
                    if s == req['stack'][-1]:
                        res_obj = obj
    except Exception as e:
        result_exception = str(e)
                 
    return jsonify(object_response(res_obj, result_exception, req['stack'], stdout))

@app.route('/get_item', methods=['GET'])
def get_item():
    global objects

    result_exception = None
    req = request.json

    print('\nSERVER: GET ITEM')
    print(req)

    try:
        base_obj = objects[req['id']]
        res_obj = base_obj[req['item']]
    except Exception as e:
        result_exception = str(e)

    return jsonify(object_response(res_obj, result_exception))

@app.route('/clear', methods=['POST', 'GET'])
def clear():
    global objects

    sc = pyspark.SparkContext.getOrCreate()
    sc.stop()

    objects = {}

    return 'ok'

def run(*args, **kwargs):
    app.run(*args, **kwargs)


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
