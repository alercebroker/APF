from .test_core import GenericConsumerTest
from apf.consumers import CSVConsumer
import unittest

import os
FILE_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLES_PATH = os.path.abspath(os.path.join(FILE_PATH,"../../examples"))


class CSVConsumerTest(GenericConsumerTest,unittest.TestCase):
    def setUp(self):
        self.component = CSVConsumer
        self.params = {
            "FILE_PATH": os.path.join(EXAMPLES_PATH,"test_csv.txt")
        }
        self.consumer = self.component(self.params)

    def test_no_path(self):
        self.assertRaises(Exception, self.component, {})
