import random
import shutil
import unittest

from base_test_case import BaseTestCase

from pyspark_proxy.sql.types import *

class TypesTestCase(BaseTestCase):
    def test_types_integer(self):
        t = IntegerType()

        self.assertEqual(type(t), IntegerType)

    def test_types_decimal(self):
        d = DecimalType(38, 18)

        self.assertEqual(str(d), 'DecimalType(38,18)')

    def test_types_array(self):
        a1 = ArrayType(StringType())
        a2 = ArrayType(StringType(), True)

        self.assertTrue(a1 == a2)
        self.assertEqual(str(a2), 'ArrayType(StringType,true)')
        self.assertEqual(a2.jsonValue(), {'containsNull': True, 'elementType': 'string', 'type': 'array'})

    def test_types_map(self):
        m1 = MapType(StringType(), IntegerType())
        m2 = MapType(StringType(), IntegerType(), True)

        self.assertTrue(m1 == m2)
        self.assertEqual(str(m1), 'MapType(StringType,IntegerType,true)')

    def test_types_struct(self):
        s1 = StructType([StructField("f1", StringType(), True)])

        self.assertEqual(str(s1['f1']), 'StructField(f1,StringType,true)')
        self.assertEqual(len(s1), 1)

if __name__ == '__main__':
    unittest.main()
