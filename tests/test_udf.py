import unittest

from base_test_case import BaseTestCase

from pyspark_proxy.sql.types import IntegerType
from pyspark_proxy.sql.functions import udf

class UdfTestCase(BaseTestCase):
    def setUp(self):
        data = [(1,2,'a'),(3,4,'b'),(5,6,'c')]
        self.df = self.sqlContext.createDataFrame(data, ['foo', 'bar', 'baz'])
        self.df.createOrReplaceTempView('foo_table')

    def test_udf_register_no_type_return(self):
        def squared(v):
            return v * v

        self.sqlContext.udf.register('squared', squared)

        df = self.sqlContext.sql('select squared(foo) AS val from foo_table').collect()

        self.assertEqual(df[1]['val'], '9')
        self.assertEqual(df[2]['val'], '25')

    def test_udf_register_typed_return(self):
        def squared(v):
            return v * v

        self.sqlContext.udf.register('squared', squared, IntegerType())

        df = self.sqlContext.sql('select squared(foo) AS val from foo_table').collect()

        self.assertEqual(df[1]['val'], 9)
        self.assertEqual(df[2]['val'], 25)

    def test_udf_from_module(self):
        from lib.my_udf import *

        self.sqlContext.udf.register('squared', squared)

        df = self.sqlContext.sql('select squared(foo) AS val from foo_table').collect()

        self.assertEqual(df[1]['val'], '9')
        self.assertEqual(df[2]['val'], '25')

    def test_udf_function(self):
        def squared(v):
            return v * v

        squared_udf = udf(squared)

        df = self.df.select(squared_udf('foo').alias('val')).collect()

        self.assertEqual(df[1]['val'], '9')
        self.assertEqual(df[2]['val'], '25')

    def test_udf_function_typed_return(self):
        def squared(v):
            return v * v

        squared_udf = udf(squared, IntegerType())

        df = self.df.select(squared_udf('foo').alias('val')).collect()

        self.assertEqual(df[1]['val'], 9)
        self.assertEqual(df[2]['val'], 25)

    def test_udf_decorator(self):
        @udf
        def squared(v):
            return v * v

        df = self.df.select(squared('foo').alias('val')).collect()

        self.assertEqual(df[1]['val'], '9')
        self.assertEqual(df[2]['val'], '25')

    def test_udf_decorator_typed_return(self):
        @udf(IntegerType())
        def squared(v):
            return v * v

        df = self.df.select(squared('foo').alias('val')).collect()

        self.assertEqual(df[1]['val'], 9)
        self.assertEqual(df[2]['val'], 25)

    def test_udf_lambda(self):
        slen = udf(lambda s: len(s), IntegerType())

        df = self.df.select(slen('baz').alias('val')).collect()

        self.assertEqual(df[1]['val'], 1)
        self.assertEqual(df[2]['val'], 1)

if __name__ == '__main__':
    unittest.main()
