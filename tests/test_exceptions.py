import unittest

from base_test_case import BaseTestCase

class ExceptionTestCase(BaseTestCase):
    def test_exception(self):
        try:
            df = self.sqlContext.read.json('/tmp/unknown/path.json')
        except Exception as e:
            self.assertIn('Path does not exist', str(e))

if __name__ == '__main__':
    unittest.main()
