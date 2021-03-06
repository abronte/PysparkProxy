import unittest

from test_spark_context import SparkContextTestCase
from test_sqlcontext import SQLContextTestCase
from test_dataframe import DataFrameTestCase
from test_column import ColumnTestCase
from test_dataframe_reader import DataFrameReaderTestCase
from test_dataframe_writer import DataFrameWriterTestCase
from test_exceptions import ExceptionTestCase
from test_udf import UdfTestCase
from test_functions import FunctionTestCase
from test_group import GroupTestCase
from test_rdd import RDDTestCase
from test_types import TypesTestCase
from test_resumable_session import ResumableSessionTestCase
from test_ml_feature import MLFeatureTestCase
from test_ml_linalg import MLLinalgTestCase

if __name__ == '__main__':
    loader = unittest.TestLoader()
    tests = [
        loader.loadTestsFromTestCase(SparkContextTestCase),
        loader.loadTestsFromTestCase(SQLContextTestCase),
        loader.loadTestsFromTestCase(DataFrameTestCase),
        loader.loadTestsFromTestCase(ColumnTestCase),
        loader.loadTestsFromTestCase(DataFrameReaderTestCase),
        loader.loadTestsFromTestCase(DataFrameWriterTestCase),
        loader.loadTestsFromTestCase(ExceptionTestCase),
        loader.loadTestsFromTestCase(UdfTestCase),
        loader.loadTestsFromTestCase(FunctionTestCase),
        loader.loadTestsFromTestCase(GroupTestCase),
        loader.loadTestsFromTestCase(RDDTestCase),
        loader.loadTestsFromTestCase(TypesTestCase),
        loader.loadTestsFromTestCase(ResumableSessionTestCase),
        loader.loadTestsFromTestCase(MLFeatureTestCase),
        loader.loadTestsFromTestCase(MLLinalgTestCase)
    ]
    suite = unittest.TestSuite(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
