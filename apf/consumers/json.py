from apf.consumers.generic import GenericConsumer


import json


class JSONConsumer(GenericConsumer):
    """JSON Consumer.

    **Example:**

    JSON Consumer configuration example

    .. code-block:: python

        #settings.py
        CONSUMER_CONFIG = { ...
            "FILEPATH": "json_file_path"
        }

    Parameters
    ----------
    FILE_PATH: path
        JSON path location

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

        msjs = []
        with open(self.config["FILE_PATH"]) as file:
            data = json.load(file)
            for row in data:
                if num_messages == 1:
                    yield row
                else:
                    msjs.append(row)
                    if len(msjs) == num_messages:
                        return_msjs = msjs.copy()
                        msjs = []
                        yield return_msjs
