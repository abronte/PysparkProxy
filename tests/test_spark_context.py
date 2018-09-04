import subprocess
import unittest
from multiprocessing import Process

from pyspark_proxy import SparkContext
from pyspark_proxy.sql import SQLContext

import pyspark_proxy.server as server

class SparkContextTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = Process(target=server.run, kwargs={'debug': True, 'use_reloader': False})
        cls.server.start()
        cls.server.join(1) # needs some time to boot up the webserver

        cls.sc = SparkContext(appName='blah')

    def test_set_log_level(self):
        self.sc.setLogLevel('ERROR')

    def test_create_sql_context(self):
        sqlContext = SQLContext(self.sc)

    @classmethod
    def tearDownClass(self):
        self.server.terminate()

if __name__ == '__main__':
    unittest.main()
