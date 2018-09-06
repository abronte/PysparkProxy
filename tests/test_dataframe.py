import os
import unittest

from base_test_case import BaseTestCase

from pyspark_proxy.server.capture import Capture

class DataFrameTestCase(BaseTestCase):
    def setUp(self):
        path = os.path.abspath(os.path.dirname(__file__)) + '/data/data.json'
        self.df = self.sqlContext.read.json(path)

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

if __name__ == '__main__':
    unittest.main()
