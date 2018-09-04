# from tests.test_
import unittest

from test_spark_context import SparkContextTestCase

if __name__ == '__main__':
    loader = unittest.TestLoader()
    tests = [
        loader.loadTestsFromTestCase(SparkContextTestCase)
    ]
    suite = unittest.TestSuite(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
