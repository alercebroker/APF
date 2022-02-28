from apf.core.step import {{step_class}} as Step
import logging


class {{step_name}}(Step):
    """{{step_name}} Description

    Parameters
    ----------
    consumer : GenericConsumer
        Description of parameter `consumer`.
    **step_args : type
        Other args passed to step (DB connections, API requests, etc.)

    """
    def __init__(self,config = None,level = logging.INFO,**step_args):
        super().__init__(config=config, level=level)
