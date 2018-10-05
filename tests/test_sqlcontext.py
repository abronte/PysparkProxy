import os
import unittest

from base_test_case import BaseTestCase

from pyspark_proxy.sql import SQLContext

class SQLContextTestCase(BaseTestCase):
    def setUp(self):
        self.data_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')

    def test_get_or_create(self):
        ctxt = SQLContext.getOrCreate(self.sc)

        self.assertEqual(ctxt.__class__, SQLContext)

        df = ctxt.read.parquet(os.path.join(self.data_path, 'data.parquet'))

        self.assertEqual(df.count(), 3)


if __name__ == '__main__':
    unittest.main()
