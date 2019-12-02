from apf.core.step import GenericStep
import logging


class {{step_name}}(GenericStep):
    """{{step_name}} Description

    Parameters
    ----------
    consumer : GenericConsumer
        Description of parameter `consumer`.
    producer : GenericProducer
        Description of parameter `producer`.
    **step_args : type
        Other args passed to step (DB connections, API requests, etc.)

    """
    def __init__(self,consumer = None, producer = None, level = logging.INFO,**step_args):
        super().__init__(consumer,producer,level=level)

    def execute(self,message):
        ################################
        #   Here comes the Step Logic  #
        ################################

        pass