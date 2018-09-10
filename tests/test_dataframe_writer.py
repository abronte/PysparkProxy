import os
import shutil
import unittest

from base_test_case import BaseTestCase

class DataFrameWriterTestCase(BaseTestCase):
    def setUp(self):
        self.output_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'test_output')
        self.json_output = os.path.join(self.output_path, 'output.json')
        self.csv_output = os.path.join(self.output_path, 'csv.json')

        data = [(1,2,'a'),(3,4,'b'),(5,6,'c')]
        self.df = self.sqlContext.createDataFrame(data, ['foo', 'bar', 'baz'])

    def tearDown(self):
        if os.path.exists(self.output_path):
            shutil.rmtree(self.output_path)

    def test_json(self):
        self.df.write.json(self.json_output)

        self.assertTrue(os.path.exists(self.json_output))

    def test_csv(self):
        self.df.write.csv(self.csv_output)

        self.assertTrue(os.path.exists(self.csv_output))

    def test_parquet(self):
        path = os.path.join(self.output_path, 'output.parquet')
        self.df.write.parquet(path)

        self.assertTrue(os.path.exists(os.path.join(path, '_SUCCESS')))

    def test_text(self):
        path = os.path.join(self.output_path, 'output.txt')
        self.df.select('baz').write.text(path)

        self.assertTrue(os.path.exists(path))

    def test_orc(self):
        path = os.path.join(self.output_path, 'output.orc')
        self.df.write.orc(path)

        self.assertTrue(os.path.exists(os.path.join(path, '_SUCCESS')))

    def test_mode(self):
        self.df.write.json(self.json_output)
        self.df.write.mode('overwrite').json(self.json_output)

        self.assertTrue(os.path.exists(self.json_output))

    def test_format(self):
        self.df.write.format('json').save(self.json_output)

        self.assertTrue(os.path.exists(self.json_output))

    def test_option(self):
        self.df.write.option('header', True).csv(self.csv_output)

        self.assertTrue(os.path.exists(self.csv_output))

    def test_options(self):
        self.df.write.options(header=True).csv(self.csv_output)

        self.assertTrue(os.path.exists(self.csv_output))

    def test_partitionby(self):
        self.df.write.partitionBy('baz').json(self.json_output)

        self.assertTrue(os.path.exists(os.path.join(self.json_output, '_SUCCESS')))
        self.assertTrue(os.path.exists(os.path.join(self.json_output, 'baz=a')))
        self.assertTrue(os.path.exists(os.path.join(self.json_output, 'baz=b')))
        self.assertTrue(os.path.exists(os.path.join(self.json_output, 'baz=c')))

    def test_partitionby(self):
        self.df.write.format('parquet').bucketBy(3, 'baz').saveAsTable('bucketed_table')
        df = self.sqlContext.sql('select * from bucketed_table')

        self.assertEquals(df.count(), 3)

    def test_sortby(self):
        self.df.write.format('parquet')\
            .bucketBy(3, 'baz')\
            .sortBy('foo')\
            .saveAsTable('sorted_table')
        df = self.sqlContext.sql('select * from sorted_table')

        self.assertEquals(df.count(), 3)

    def test_save(self):
        self.df.write.save(self.json_output, format='json')

        self.assertTrue(os.path.exists(self.json_output))

    def test_insert_into(self):
        self.df.write.format('parquet').saveAsTable('temp_table')

        data = [(1,2,'a'),(3,4,'b'),(5,6,'c')]
        df = self.sqlContext.createDataFrame(data, ['foo', 'bar', 'baz'])

        df.write.insertInto('temp_table')

        inserted_df = self.sqlContext.table('temp_table')

        self.assertEqual(inserted_df.count(), 6)

    def test_save_as_table(self):
        self.df.write.saveAsTable('save_as_table', format='parquet')
        df = self.sqlContext.table('save_as_table')

        self.assertEqual(df.count(), 3)

if __name__ == '__main__':
    unittest.main()
