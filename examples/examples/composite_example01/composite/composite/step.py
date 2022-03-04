from apf.core.step import CompositeStep as Step
import logging
from pathlib import Path
import importlib
import sys

this_path = str(Path(__file__).parent.resolve())


class Composite(Step):
    """Composite Description

    Parameters
    ----------
    config : dict
        Configuration for the step and its components
    **step_args : type
        Other args passed to step (DB connections, API requests, etc.)

    """

    def __init__(self, config=None, level=logging.INFO, **step_args):
        super().__init__(config=config, level=level)

    def import_step(self, step_name):
        del sys.modules["settings"]
        spec = importlib.util.spec_from_file_location(
            step_name,
            this_path + f"/../../{ step_name }/scripts/run_step.py",
        )
        step = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(step)
        return step

    def execute(self, message):
        """This is an anti pattern.
        One should not override execute method on Composite steps,
        but this example uses CSV Consumers,
        so there is no other way to use it yet.
        """
        self._internal_produce(message)
        step = self.import_step("component1")
        step.main()
        step = self.import_step("component2")
        step.main()
        return self._internal_consume()
