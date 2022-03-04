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
        self.path = self.config.get("FILE_PATH", None)
        if self.path is None:
            raise Exception("FILE_PATH variable not set")

    def consume(self):
        paths = []
        if isinstance(self.path, list):
            for path in self.path:
                paths.append(path)
        else:
            paths.append(self.path)
        for path in paths:
            df = pd.read_csv(path, **self.config.get("OTHER_ARGS", {}))
            self.len = len(df)
            for index, row in df.iterrows():
                yield row.to_dict()
