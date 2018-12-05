import unittest

from base_test_case import BaseTestCase

from pyspark_proxy.ml.linalg import *

class MLLinalgTestCase(BaseTestCase):
    def test_ml_linalg_dense_vector(self):
        dv = DenseVector([1.0, 2.0])

        self.assertEqual(type(dv), DenseVector)

if __name__ == '__main__':
    unittest.main()
