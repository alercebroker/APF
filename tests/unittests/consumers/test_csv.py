from test_core import GenericConsumerTest
from apf.consumers import CSVConsumer
import unittest

import os

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLES_PATH = os.path.abspath(os.path.join(FILE_PATH, "../../examples"))


class CSVConsumerTest(GenericConsumerTest, unittest.TestCase):
    component = CSVConsumer
    params = {"FILE_PATH": os.path.join(EXAMPLES_PATH, "test_csv.txt")}

    def test_no_path(self):
        self.assertRaises(Exception, self.component, {})

    def test_batch(self):
        config = {
            "FILE_PATH": os.path.join(EXAMPLES_PATH, "test_csv.txt"),
            "consume.messages": 2,
        }
        consumer = self.component(config)
        for msgs in consumer.consume():
            self.assertEqual(len(msgs), 2)
