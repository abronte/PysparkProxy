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

        data = [(1,2,'a'),(3,4,'b'),(5,6,'c')]
        self.df = self.sqlContext.createDataFrame(data, ['foo', 'bar', 'baz'])

    def tearDown(self):
        if os.path.exists(self.output_path):
            shutil.rmtree(self.output_path)

    def test_column_get_item(self):
        c = self.df['bar']

        self.assertEqual(type(c), Column)

    def test_column_get_attr(self):
        c = self.df.bar

        self.assertEqual(type(c), Column)

    def test_select_with_column(self):
        res_df = self.df.select(self.df['bar'], self.df.foo)

        self.assertEqual(res_df.columns, ['bar', 'foo'])

    def test_column_alias(self):
        res_df = self.df.select(self.df['foo'].alias('new_name'))

        self.assertEqual(res_df.columns, ['new_name'])

    def test_column_cast(self):
        res_df = self.df.select(self.df['foo'].cast('string'))

        with Capture() as output:
            res_df.printSchema()

        self.assertIn('string', '\n'.join(output))

    def test_column_add(self):
        res = self.df.withColumn('add', self.df.foo + 1).collect()

        self.assertEqual(res[0]['add'], 2)
        self.assertEqual(res[1]['add'], 4)
        self.assertEqual(res[2]['add'], 6)

    def test_column_radd(self):
        res = self.df.withColumn('add', self.df.foo + self.df.bar).collect()

        self.assertEqual(res[0]['add'], 3)
        self.assertEqual(res[1]['add'], 7)
        self.assertEqual(res[2]['add'], 11)

    def test_column_rsubtract(self):
        res = self.df.withColumn('sub', self.df.foo - self.df.bar).sort('sub').collect()

        self.assertEqual(res[0]['sub'], -1)
        self.assertEqual(res[1]['sub'], -1)
        self.assertEqual(res[2]['sub'], -1)

    def test_column_eq(self):
        res = self.df.filter(self.df.foo == 1).collect()

        self.assertEqual(res[0]['foo'], 1)

    def test_column_gt(self):
        res = self.df.filter(self.df.foo > 1).sort('foo').collect()

        self.assertEqual(res[0]['foo'], 3)
        self.assertEqual(res[1]['foo'], 5)

    def test_column_ne_and(self):
        res = self.df.filter((self.df.foo != 1) & (self.df.foo != 5)).collect()

        self.assertEqual(res[0]['foo'], 3)

    def test_column_or(self):
        res = self.df.filter((self.df.foo == 1) | (self.df.foo == 5)).collect()

        self.assertEqual(res[0]['foo'], 1)
        self.assertEqual(res[1]['foo'], 5)

if __name__ == '__main__':
    unittest.main()
