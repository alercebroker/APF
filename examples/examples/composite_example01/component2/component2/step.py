from apf.core.step import ComponentStep as Step
import logging


class Component2(Step):
    """Component2 Description

    Parameters
    ----------
    consumer : GenericConsumer
        Description of parameter `consumer`.
    **step_args : type
        Other args passed to step (DB connections, API requests, etc.)

    """

    def __init__(self, config=None, level=logging.INFO, **step_args):
        super().__init__(config=config, level=level)

    def execute(self, message):
        for k in message:
            message[k] = int(message[k]) + 2
        return message
