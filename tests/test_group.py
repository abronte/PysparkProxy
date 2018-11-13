import sys
import os
import shutil
import unittest
import pandas

from base_test_case import BaseTestCase

import pyspark_proxy.sql.functions as F

class GroupTestCase(BaseTestCase):
    def test_group_group_by_agg(self):
        data = [(1,2,'a'),(3,4,'a'),(5,6,'c')]
        df = self.sqlContext.createDataFrame(data, ['foo', 'bar', 'baz'])

        res = df.groupBy('baz').agg(F.sum('foo').alias('summed')).sort('summed').collect()

        self.assertEqual(res[0]['summed'], 4)
        self.assertEqual(res[1]['summed'], 5)

    def test_group_count(self):
        data = [(1,2,'a'),(3,4,'a'),(5,6,'c')]
        df = self.sqlContext.createDataFrame(data, ['foo', 'bar', 'baz'])

        res = df.groupBy(df.foo).count().collect()

        self.assertEqual(res[0]['count'], 1)
        self.assertEqual(res[1]['count'], 1)
        self.assertEqual(res[2]['count'], 1)

    def test_group_pivot(self):
        data = [(1,2,'a'),(3,4,'a'),(5,6,'c')]
        df = self.sqlContext.createDataFrame(data, ['foo', 'bar', 'baz'])

        res = df.groupBy('foo').pivot('baz', ['a', 'c']).sum('bar').collect()

        self.assertEqual(res[0]['foo'], 5)
        self.assertEqual(res[0]['c'], 6)

if __name__ == '__main__':
    unittest.main()
