import os
import random
import shutil
import unittest

from base_test_case import BaseTestCase

from pyspark_proxy.rdd import RDD

class RDDTestCase(BaseTestCase):
    def setUp(self):
        self.current_path = os.path.abspath(os.path.dirname(__file__))
        self.file_path = os.path.join(self.current_path, 'data', 'data.txt')
        self.output_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'test_output')

    def tearDown(self):
        if os.path.exists(self.output_path):
            shutil.rmtree(self.output_path)

    def test_rdd_load_file(self):
        rdd = self.sc.textFile(self.file_path)

        self.assertEqual(type(rdd), RDD)

    def test_rdd_flatmap(self):
        rdd = self.sc.textFile(self.file_path)

        res = rdd.flatMap(lambda line: line.split(',')).collect()

        self.assertEqual(res, [u'1', u'2', u'3', u'4', u'5', u'6'])

    def test_rdd_complex(self):
        rdd = self.sc.textFile(self.file_path)

        res = rdd.flatMap(lambda line: line.split(','))\
                .map(lambda word: (word, 1))\
                .reduceByKey(lambda a, b: a + b)\
                .collect()

        assert_result = [(u'1', 1), (u'3', 1), (u'5', 1), (u'2', 1), (u'4', 1), (u'6', 1)]
        self.assertEqual(res, assert_result)

    def test_rdd_parallelize_and_filter(self):
        NUM_SAMPLES = 5

        def inside(p):
            x, y = random.random(), random.random()
            return x*x + y*y < 1

        count = self.sc.parallelize(range(0, NUM_SAMPLES)) \
            .filter(inside).count()

        self.assertEqual(type(count), int)

    def test_rdd_write_file(self):
        path = os.path.join(self.output_path, 'rdd_write.txt')
        rdd = self.sc.parallelize([1,2,3,4,5])
        
        rdd.saveAsTextFile(path)

        self.assertTrue(os.path.exists(path))

if __name__ == '__main__':
    unittest.main()
