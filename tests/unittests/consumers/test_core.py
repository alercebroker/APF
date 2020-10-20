from apf.consumers import GenericConsumer

import unittest

class GenericConsumerTest():

    def test_consume(self):
        for msj in self.consumer.consume():
            self.assertIsInstance(msj, dict)
            self.consumer.commit()
