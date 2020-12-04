from apf.consumers.generic import GenericConsumer
import pandas as pd


class CSVConsumer(GenericConsumer):
    """CSV Consumer.

    **Example:**

    CSV Consumer configuration example

    .. code-block:: python

        #settings.py
        CONSUMER_CONFIG = { ...
            "FILE_PATH": "csv_file_path",
            "OTHER_ARGS": {
                "index_col": "id",
                "sep": ";",
                "header": 0
            }
        }

    Parameters
    ----------
    FILE_PATH: path
        CSV path location

    OTHER_ARGS: dict
        Parameters passed to :func:`pandas.read_csv`
        (reference `here <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html>`_)

    """

    def __init__(self, config):
        super().__init__(config)
        path = self.config.get("FILE_PATH", None)
        if path is None:
            raise Exception("FILE_PATH variable not set")

    def consume(self):
        if "consume.messages" in self.config:
            num_messages = self.config["consume.messages"]
        elif "NUM_MESSAGES" in self.config:
            num_messages = self.config["NUM_MESSAGES"]
        else:
            num_messages = 1

        msgs = []
        df = pd.read_csv(self.config["FILE_PATH"], **self.config.get("OTHER_ARGS", {}))
        self.len = len(df)
        for index, row in df.iterrows():
            if num_messages == 1:
                yield row.to_dict()
            else:
                msgs.append(row.to_dict())
                if len(msgs) == num_messages:
                    return_msgs = msgs.copy()
                    msjs = []
                    yield return_msgs
