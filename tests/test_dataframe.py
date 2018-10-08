import sys
import os
import shutil
import unittest
import pandas

from base_test_case import BaseTestCase

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

if __name__ == '__main__':
    unittest.main()
