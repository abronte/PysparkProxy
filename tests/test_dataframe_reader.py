import os
import unittest

from base_test_case import BaseTestCase

class DataFrameReaderTestCase(BaseTestCase):
    def setUp(self):
        self.data_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')
        self.json_path = os.path.join(self.data_path, 'data.json')
        self.csv_path = os.path.join(self.data_path, 'data.csv')

    def test_read_json(self):
        df = self.sqlContext.read.json(self.json_path)

        self.assertEqual(df.count(), 3)

    def test_read_csv(self):
        df = self.sqlContext.read.csv(self.csv_path, header=True)

        self.assertEqual(df.count(), 3)

    def test_read_text(self):
        df = self.sqlContext.read.text(os.path.join(self.data_path, 'data.txt'))

        self.assertEqual(df.count(), 3)

    def test_read_parquet(self):
        df = self.sqlContext.read.parquet(os.path.join(self.data_path, 'data.parquet'))

        self.assertEqual(df.count(), 3)

    def test_read_orc(self):
        df = self.sqlContext.read.orc(os.path.join(self.data_path, 'data.orc'))

        self.assertEqual(df.count(), 3)

    def test_option(self):
        df = self.sqlContext.read.option('header', True).csv(self.csv_path)

        self.assertEqual(df.count(), 3)

    def test_options(self):
        df = self.sqlContext.read.options(header=True).csv(self.csv_path)

        self.assertEqual(df.count(), 3)

    def test_format(self):
        df = self.sqlContext.read.format('json').load(self.json_path)

        self.assertEqual(df.count(), 3)

    def test_load(self):
        df = self.sqlContext.read.load(self.json_path, format='json')

        self.assertEqual(df.count(), 3)

    def test_load_multiple_paths(self):
        df = self.sqlContext.read.format('json')\
            .load([self.json_path, self.json_path])

        self.assertEqual(df.count(), 6)

    def test_table(self):
        df = self.sqlContext.read.parquet(os.path.join(self.data_path, 'data.parquet'))
        df.createOrReplaceTempView('tmpTable')

        df_table = self.sqlContext.read.table('tmpTable')
        self.assertEqual(df_table.count(), 3)

    def test_schema(self):
        s = self.sqlContext.read.schema("foo STRING, bar INT")
        df = s.json(self.json_path)
        df_types = df.dtypes

        self.assertEqual(df_types[0][1], 'string')
        self.assertEqual(df_types[1][1], 'int')

if __name__ == '__main__':
    unittest.main()
