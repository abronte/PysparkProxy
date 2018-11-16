import hashlib
import logging

logger = logging.getLogger()

from flask import request

request_responses = {}

def before():
    global request_responses

    # if RESUMABLE:
    req_digest = hashlib.sha1(str(request.json)).hexdigest()

    if req_digest in request_responses:
        logger.info('RESUMABLE: %s - already run, returning result' % req_digest)
        return request_responses[req_digest]
    else:
        logger.info('RESUMABLE: %s - not cached, running' % req_digest)

def after(response):
    global request_responses

    # if RESUMABLE:
    req_digest = hashlib.sha1(str(request.json)).hexdigest()
    request_responses[req_digest] = response

    return response
