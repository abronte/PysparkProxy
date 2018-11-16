import uuid
import os
import cloudpickle
import pickle
import base64
import logging
import types

logger = logging.getLogger()

import findspark
findspark.init()

import pyspark
import pyspark.sql.functions

from flask import Flask, request, jsonify
from flask.logging import default_handler

from pyspark_proxy.server.capture import Capture

app = Flask(__name__)

objects = {}

# looks at incomming arguments to see if there are any
# pyspark objects that should be retrieved and passed in
def arg_objects(request_args, request_kwargs={}):
    global objects

    args = []
    kwargs = {}

    for a in request_args:
        if type(a) == dict:
            if '_CLOUDPICKLE' in a:
                obj = cloudpickle.loads(base64.b64decode(a['_CLOUDPICKLE']))
                args.append(obj)
            else:
                args.append(retrieve_object(a))
        # pyspark objects can sometimes be in lists so we need to
        # check the list and lookup any objects
        elif type(a) == list:
            processed_list = []

            for x in a:
                processed_list.append(retrieve_object(x))

            args.append(processed_list)
        # spark strictly typechecks some arguments expecting strings but
        # decoding json will turn strings into unicode objects
        elif type(a) == unicode:
            args.append(str(a))
        else:
            args.append(a)

    for k in request_kwargs:
        v = request_kwargs[k]

        kwargs[k] = retrieve_object(v)

    return args, kwargs

def retrieve_object(obj):
    if type(obj) == dict and '_PROXY_ID' in obj:
        id = obj['_PROXY_ID']
        logger.debug('Retrieving object id: %s' % id)

        return objects[id]
    else:
        return obj

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

        if 'pyspark' in str(obj.__class__) or type(obj) == types.FunctionType:
            id = str(uuid.uuid4())

            result['id'] = id

            logger.info('Adding object id %s to the stack' % id)
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

    logger.info('/create')
    logger.info(req)

    id = str(uuid.uuid4())
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
    args, kwargs = arg_objects(req['args'], req['kwargs'])
    objects[id] = callable(*args, **kwargs)

    return jsonify({'object': True, 'id': id})

@app.route('/call', methods=['POST'])
def call():
    global objects
    
    req = request.json

    logger.info('/call')
    logger.info(req)

    result_exception = None
    res_obj = None
    paths = []

    if req['id'] == 'pyspark':
        base_obj = pyspark
    else:
        base_obj = objects[req['id']]

    if req['path'] != None:
        paths = req['path'].split('.')

    func = base_obj
    args, kwargs = arg_objects(req['args'], req['kwargs'])

    try:
        for p in paths:
            func = getattr(func, p)
        
        with Capture() as stdout:
            if callable(func):
                res_obj = func(*args, **kwargs)
            else:
                res_obj = func
    except Exception as e:
        result_exception = str(e)

    return jsonify(object_response(res_obj, result_exception, paths, stdout))


@app.route('/call_chain', methods=['POST'])
def call_chain():
    global objects

    req = request.json

    logger.info('/call_chain')
    logger.info(req)

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

@app.route('/call_class_method', methods=['POST'])
def call_class_method():
    global objects

    req = request.json

    logger.info('/call_call_method')
    logger.info(req)

    res_obj = None
    result_exception = None

    module_paths = req['module'].split('.')
    base_module = __import__(module_paths[0])

    module = base_module

    if len(module_paths) > 1:
        for m in module_paths[1:]:
            module = getattr(module, m)

    callable = getattr(module, req['class'])
    args, kwargs = arg_objects(req['args'], req['kwargs'])

    # should this capture stdout?
    try:
        res_obj = callable(*args, **kwargs)
    except Exception as e:
        result_exception = str(e)

    return jsonify(object_response(res_obj, result_exception, module_paths))

@app.route('/get_item', methods=['GET'])
def get_item():
    global objects

    result_exception = None
    res_obj = None
    req = request.json

    logger.info('/get_item')
    logger.info(req)

    try:
        base_obj = objects[req['id']]
        res_obj = base_obj[req['item']]
    except Exception as e:
        result_exception = str(e)

    return jsonify(object_response(res_obj, result_exception))

@app.route('/clear', methods=['POST', 'GET'])
def clear():
    global objects

    logger.info('/clear')

    sc = pyspark.SparkContext.getOrCreate()
    sc.stop()

    objects = {}

    return 'ok'

def run(*args, **kwargs):
    if 'debug' not in kwargs or ('debug' in kwargs and kwargs['debug'] == False):
        app.logger.removeHandler(default_handler)
        app.logger = logger

        logger.info('Starting pyspark proxy web server')

    if 'port' not in kwargs:
        kwargs['port'] = 8765

    if 'resumable' in kwargs and kwargs['resumable'] == True:
        import pyspark_proxy.server.resumable as resumable

        app.before_request(resumable.before)
        app.after_request(resumable.after)

        del kwargs['resumable']

    app.run(*args, **kwargs)

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, port=8765)
