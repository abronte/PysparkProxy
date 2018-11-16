import unittest

from base_test_case import BaseTestCase

from pyspark_proxy import SparkContext
from pyspark_proxy.sql import SQLContext

class ResumableSessionTestCase(BaseTestCase):
    def test_resumable_spark_context(self):
        sc = SparkContext(appName='test_case')

        self.assertEqual(sc._id, self.sc._id)

if __name__ == '__main__':
    unittest.main()
