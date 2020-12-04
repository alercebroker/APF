from apf.consumers import GenericConsumer

import unittest


class GenericConsumerTest(unittest.TestCase):
    component = GenericConsumer
    params = {}

    def test_consume(self):
        comp = self.component(self.params)
        for msj in comp.consume():
            self.assertIsInstance(msj, dict)
            comp.commit()

    def test_consume_batch(self):
        params = self.params.copy()
        params["consume.messages"] = 2
        comp = self.component(params)
        for msg in comp.consume():
            self.assertIsInstance(msg, list)
            comp.commit()
