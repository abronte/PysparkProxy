import subprocess
import unittest
from multiprocessing import Process
import time

import requests

from pyspark_proxy import SparkContext
from pyspark_proxy.sql import SQLContext

import pyspark_proxy.server as server

class BaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = Process(target=server.run, kwargs={'debug': True, 'use_reloader': False})
        cls.server.start()
        cls.server.join(1) # needs some time to boot up the webserver

        cls.sc = SparkContext(appName='test_case')
        cls.sc.setLogLevel('ERROR')
        cls.sqlContext = SQLContext(cls.sc)

    @classmethod
    def tearDownClass(self):
        #since the tests are being run in a spark application the
        #spark context needs to be manually killed since killing the proc
        #doesn't kill the contexts created
        requests.get('http://localhost:8765/clear')

        self.server.terminate()
