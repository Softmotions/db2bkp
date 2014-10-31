from configparser import ConfigParser
import unittest
import os

from db2bkp.utils.EnvInterpolation import EnvInterpolation


class DB2BKPTest(unittest.TestCase):
    def testConfig(self):
        env = {'foo': 'fooVal'}
        cfg = ConfigParser(interpolation=EnvInterpolation(env), allow_no_value=True, strict=False)
        cfg.read(os.path.join(os.path.dirname(__file__), 'test.cfg'))
        ts = cfg['Test']
        self.assertIsNotNone(ts)
        self.assertEqual(ts['key1'], 'val1')
        self.assertEqual('prefix fooVal postfix', ts['key2'])


if __name__ == '__main__':
    unittest.main()
