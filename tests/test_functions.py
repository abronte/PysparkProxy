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

    def test_function_count_distinct(self):
        res = self.df.agg(F.countDistinct('foo', 'bar').alias('cnt')).collect()

        self.assertEqual(res[0]['cnt'], 3)

    def test_function_input_file_name(self):
        res = self.df.select(F.input_file_name().alias('f')).collect()

        self.assertEqual(res[0]['f'], '')

    def test_function_concat(self):
        res = self.df.select(F.concat(self.df.bar, self.df.baz).alias('concated')).collect()

        self.assertEqual(res[0]['concated'], '2a')
        self.assertEqual(res[1]['concated'], '4b')
        self.assertEqual(res[2]['concated'], '6c')

    def test_function_concat(self):
        res = self.df.select(F.sha2(self.df.baz, 256).alias('sha2')).collect()

        self.assertEqual(res[0]['sha2'], 'ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb')
        self.assertEqual(res[1]['sha2'], '3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d')
        self.assertEqual(res[2]['sha2'], '2e7d2c03a9507ae265ecf5b5356885a53393a2029d241394997265a1a25aefc6')

if __name__ == '__main__':
    unittest.main()
