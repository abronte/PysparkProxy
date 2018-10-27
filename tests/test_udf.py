import unittest

from base_test_case import BaseTestCase

from pyspark_proxy.sql.types import IntegerType

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

if __name__ == '__main__':
    unittest.main()
