import os
import unittest

from base_test_case import BaseTestCase

class DataFrameTestCase(BaseTestCase):
    def test_count(self):
        path = os.path.abspath(os.path.dirname(__file__)) + '/data/data.json'
        df = self.sqlContext.read.json(path)

        self.assertEqual(df.count(), 3)

if __name__ == '__main__':
    unittest.main()
