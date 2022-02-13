from apf.core.step import GenericStep
import logging


class LifecycleExample01(GenericStep):
    """LifecycleExample01 Description

    Parameters
    ----------
    consumer : GenericConsumer
        Description of parameter `consumer`.
    **step_args : type
        Other args passed to step (DB connections, API requests, etc.)

    """

    def __init__(self, config=None, level=logging.INFO, **step_args):
        super().__init__(config=config, level=level)

    def pre_consume(self):
        self.logger.info("I'm pre_consume")

    def pre_execute(self, message):
        self.logger.info("I'm pre_execute")

    def execute(self, message):
        self.logger.info("I'm execute")

    def post_execute(self, result):
        self.logger.info("I'm post_execute")

    def pre_produce(self, result):
        self.logger.info("I'm pre_produce")

    def post_produce(self):
        self.logger.info("I'm post_produce")

    def tear_down(self):
        self.logger.info("I'm tear_down")
