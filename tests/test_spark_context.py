import unittest

from base_test_case import BaseTestCase

class SparkContextTestCase(BaseTestCase):
    def test_set_log_level(self):
        self.sc.setLogLevel('ERROR')

    def test_get_hadoop_config(self):
        self.sc._jsc.hadoopConfiguration().set('foo', 'bar')
        val = self.sc._jsc.hadoopConfiguration().get('foo')

        self.assertEqual(val, 'bar')

if __name__ == '__main__':
    unittest.main()
