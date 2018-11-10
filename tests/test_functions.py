import unittest

from base_test_case import BaseTestCase

from pyspark_proxy.sql.dataframe import DataFrame
import pyspark_proxy.sql.functions as F

class FunctionTestCase(BaseTestCase):
    def setUp(self):
        data = [(1,2,'a'),(3,4,'b'),(5,6,'c')]
        self.df = self.sqlContext.createDataFrame(data, ['foo', 'bar', 'baz'])
        self.df.createOrReplaceTempView('foo_table')

    def test_function_lit(self):
        res = self.df.select(F.lit('foobar').alias('lit')).collect()

        self.assertEqual(res[0]['lit'], 'foobar')

    def test_function_count(self):
        res = self.df.select(F.count('foo').alias('cnt')).collect()

        self.assertEqual(res[0]['cnt'], 3)

    def test_function_collect_list(self):
        res = self.df.select(F.collect_list('foo').alias('collected')).collect()

        self.assertEqual(res[0]['collected'], [1,3,5])

    def test_function_approx_count_distinct(self):
        res = self.df.agg(F.approx_count_distinct('foo').alias('cnt')).collect()

        self.assertEqual(res[0]['cnt'], 3)

    def test_function_broadcast(self):
        df = F.broadcast(self.df)

        self.assertEqual(type(df), DataFrame)

if __name__ == '__main__':
    unittest.main()
