from multiprocessing import Process
import unittest
import requests

from base_test_case import BaseTestCase

from pyspark_proxy import SparkContext
from pyspark_proxy.sql import SQLContext

import pyspark_proxy.server as server

class ResumableSessionTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = Process(target=server.run, kwargs={'resumable': True, 'debug': True, 'use_reloader': False})
        cls.server.start()
        cls.server.join(1) # needs some time to boot up the webserver

        cls.sc = SparkContext(appName='test_case')
        cls.sc.setLogLevel('ERROR')
        cls.sqlContext = SQLContext(cls.sc)

    @classmethod
    def tearDownClass(self):
        requests.get('http://localhost:8765/clear')

        self.server.terminate()

    def test_resumable_spark_context(self):
        sc = SparkContext(appName='test_case')

        self.assertEqual(sc._id, self.sc._id)

    def test_resumable_create_dataframe(self):
        data = [(1,2,'a'),(3,4,'b'),(5,6,'c')]

        df1 = self.sqlContext.createDataFrame(data, ['foo', 'bar', 'baz'])
        df2 = self.sqlContext.createDataFrame(data, ['foo', 'bar', 'baz'])
        df3 = self.sqlContext.createDataFrame(data, ['a', 'b', 'c'])

        self.assertEqual(df1._id, df2._id)
        self.assertNotEqual(df2._id, df3._id)

if __name__ == '__main__':
    unittest.main()
