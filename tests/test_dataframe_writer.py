import os
import shutil
import unittest

from base_test_case import BaseTestCase

class DataFrameWriterTestCase(BaseTestCase):
    def setUp(self):
        self.output_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'test_output')

        self.df = self.sqlContext.createDataFrame([(1,2),(3,4),(5,6)], ['foo', 'bar'])

    def tearDown(self):
        if os.path.exists(self.output_path):
            shutil.rmtree(self.output_path)

    def test_json(self):
        path = os.path.join(self.output_path, 'output.json')
        self.df.write.json(path)

        self.assertTrue(os.path.exists(path))

    def test_csv(self):
        path = os.path.join(self.output_path, 'output.csv')
        self.df.write.csv(path)

        self.assertTrue(os.path.exists(path))

    def test_parquet(self):
        path = os.path.join(self.output_path, 'output.parquet')
        self.df.write.parquet(path)

        self.assertTrue(os.path.exists(os.path.join(path, '_SUCCESS')))

if __name__ == '__main__':
    unittest.main()
