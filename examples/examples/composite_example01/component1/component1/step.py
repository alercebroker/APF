from apf.core.step import ComponentStep as Step
import logging


class Component1(Step):
    """Component1 Description

    Parameters
    ----------
    config : dict
        Configuration for the step and its components
    **step_args : type
        Other args passed to step (DB connections, API requests, etc.)

    """

    def __init__(self, config=None, level=logging.INFO, **step_args):
        super().__init__(config=config, level=level)
        self.logger.info(config["PRODUCER_CONFIG"])

    def execute(self, message):
        self.logger.info(message)
        return message
