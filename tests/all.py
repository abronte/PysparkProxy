# from tests.test_
import unittest

from test_spark_context import SparkContextTestCase
from test_dataframe import DataFrameTestCase

if __name__ == '__main__':
    loader = unittest.TestLoader()
    tests = [
        loader.loadTestsFromTestCase(SparkContextTestCase),
        loader.loadTestsFromTestCase(DataFrameTestCase)
    ]
    suite = unittest.TestSuite(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
