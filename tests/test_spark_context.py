import unittest

from base_test_case import BaseTestCase

class SparkContextTestCase(BaseTestCase):
    def test_set_log_level(self):
        self.sc.setLogLevel('ERROR')

if __name__ == '__main__':
    unittest.main()
