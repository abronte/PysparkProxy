import sys
import os
import shutil
import unittest
import pandas

from base_test_case import BaseTestCase

import pyspark_proxy.sql.functions as F
from pyspark_proxy.server.capture import Capture

class DataFrameTestCase(BaseTestCase):
    def setUp(self):
        self.current_path = os.path.abspath(os.path.dirname(__file__))
        self.output_path = os.path.join(self.current_path, 'test_output')

        self.df = self.sqlContext.read.json(os.path.join(self.current_path, 'data', 'data.json'))

    def tearDown(self):
        if os.path.exists(self.output_path):
            shutil.rmtree(self.output_path)

    def test_count(self):
        self.assertEqual(self.df.count(), 3)

    def test_temp_table(self):
        self.df.createOrReplaceTempView('my_table')

        results = self.sqlContext.sql('select count(*) from my_table')

        self.assertEqual(results.count(), 1)

    def test_show(self):
        with Capture() as output:
            self.df.show()

        expected_output = [
            u'+---+---+',
            u'|bar|foo|',
            u'+---+---+',
            u'|  2|  1|',
            u'|  4|  3|',
            u'|  6|  5|',
            u'+---+---+',
            u'']

        self.assertEqual(expected_output, output)

        with Capture() as output:
            self.df.show(1)

        expected_output = [
            u'+---+---+',
            u'|bar|foo|',
            u'+---+---+',
            u'|  2|  1|',
            u'+---+---+',
            u'only showing top 1 row',
            u'']

        self.assertEqual(expected_output, output)

    def test_to_pandas(self):
        pdf = self.df.toPandas()

        self.assertEqual(type(pdf), pandas.core.frame.DataFrame)

    def test_collect(self):
        rows = self.df.collect()

        self.assertEqual(rows[0]['foo'], 1)

    def test_columns(self):
        cols = self.df.columns

        self.assertEqual(cols, ['bar', 'foo'])

    def test_dtypes(self):
        dtypes = self.df.dtypes

        self.assertEqual(dtypes, [[u'bar', u'bigint'], [u'foo', u'bigint']])

    def test_dataframe_with_column(self):
        res = self.df.withColumn('new_col', F.lit('new_col')).collect()

        self.assertEqual(res[0]['new_col'], 'new_col')

    def test_dataframe_join(self):
        df1 = self.sqlContext.createDataFrame([(1,2,'value 1')], ['id1', 'id2', 'val'])
        df2 = self.sqlContext.createDataFrame([(1,2,'value 2')], ['id1', 'id2', 'val'])

        res = df1.join(df2, df1.id1 == df2.id1, 'left').select(df1.val).collect()

        self.assertEqual(res[0]['val'], 'value 1')

    def test_dataframe_join_multiple_columns(self):
        df1 = self.sqlContext.createDataFrame([(1,2,'value 1')], ['id1', 'id2', 'val'])
        df2 = self.sqlContext.createDataFrame([(1,2,'value 2')], ['id1', 'id2', 'val'])

        cond = [
                F.sha2(df1.id1.cast('string'), 256) == F.sha2(df2.id1.cast('string'), 256),
                df1.id2 == df2.id2]

        res = df1.join(df2, cond, 'left').select(df1.val).collect()

        self.assertEqual(res[0]['val'], 'value 1')

    def test_dataframe_repr(self):
        df_str = str(self.df)

        self.assertEqual(df_str, 'DataFrame[bar: bigint, foo: bigint]')

if __name__ == '__main__':
    unittest.main()
