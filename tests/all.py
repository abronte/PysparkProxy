import unittest

from test_spark_context import SparkContextTestCase
from test_dataframe import DataFrameTestCase
from test_column import ColumnTestCase
from test_dataframe_reader import DataFrameReaderTestCase
from test_dataframe_writer import DataFrameWriterTestCase
from test_exceptions import ExceptionTestCase

if __name__ == '__main__':
    loader = unittest.TestLoader()
    tests = [
        loader.loadTestsFromTestCase(SparkContextTestCase),
        loader.loadTestsFromTestCase(DataFrameTestCase),
        loader.loadTestsFromTestCase(ColumnTestCase),
        loader.loadTestsFromTestCase(DataFrameReaderTestCase),
        loader.loadTestsFromTestCase(DataFrameWriterTestCase),
        loader.loadTestsFromTestCase(ExceptionTestCase)
    ]
    suite = unittest.TestSuite(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
