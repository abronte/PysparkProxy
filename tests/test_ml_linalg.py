import unittest

from base_test_case import BaseTestCase

from pyspark_proxy.ml.linalg import *

class MLLinalgTestCase(BaseTestCase):
    def test_ml_linalg_dense_vector(self):
        dv = DenseVector([1.0, 2.0])

        self.assertEqual(type(dv), DenseVector)

    def test_ml_linalg_dense_vector_dataframe_nested(self):
        df = self.sqlContext.createDataFrame([(DenseVector([1.0, 2.0]),),(DenseVector([0.0, 1.0]),),(DenseVector([3.0, 0.2]),)], ["tf"])
        row = df.head()

        self.assertIn('DenseVector', str(type(row.tf)))

if __name__ == '__main__':
    unittest.main()
