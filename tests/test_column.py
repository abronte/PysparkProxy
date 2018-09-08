import os
import shutil
import unittest
import pandas

from base_test_case import BaseTestCase

from pyspark_proxy.sql.column import Column
from pyspark_proxy.server.capture import Capture

class ColumnTestCase(BaseTestCase):
    def setUp(self):
        self.current_path = os.path.abspath(os.path.dirname(__file__))
        self.output_path = os.path.join(self.current_path, 'test_output')

        self.df = self.sqlContext.read.json(os.path.join(self.current_path, 'data', 'data.json'))

    def tearDown(self):
        if os.path.exists(self.output_path):
            shutil.rmtree(self.output_path)

    def test_column_get_item(self):
        c = self.df['bar']

        self.assertEqual(type(c), Column)

    def test_select_with_column(self):
        c = self.df['bar']
        res_df = self.df.select(c)

        self.assertEqual(res_df.columns, ['bar'])

    def test_column_alias(self):
        res_df = self.df.select(self.df['foo'].alias('new_name'))

        self.assertEqual(res_df.columns, ['new_name'])

    def test_column_cast(self):
        res_df = self.df.select(self.df['foo'].cast('string'))

        with Capture() as output:
            res_df.printSchema()

        self.assertIn('string', '\n'.join(output))

if __name__ == '__main__':
    unittest.main()
