from apf.core.step import SimpleStep as Step
import logging


class SimpleExample01(Step):
    """SimpleExample01 Description

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
        ################################
        #   Here comes the Step Logic  #
        ################################

        pass
