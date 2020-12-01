from apf.consumers.generic import GenericConsumer

import fastavro
import os
import glob


class AVROFileConsumer(GenericConsumer):
    """Consume from a AVRO Files Directory.

    **Example:**

    .. code-block:: python

        #settings.py
        CONSUMER_CONFIG = { ...
            "DIRECTORY_PATH": "path/to/avro/directory"
        }

    Parameters
    ----------
    DIRECTORY_PATH: path
        AVRO files Directory path location

    """

    def __init__(self, config):
        super().__init__(config)

    def consume(self):
        files = glob.glob(os.path.join(self.config["DIRECTORY_PATH"], "*.avro"))
        files.sort()
        if len(files) == 0:
            raise (f"No files found in {self.config['DIRECTORY_PATH']}")

        if "consume.messages" in self.config:
            num_messages = self.config["consume.messages"]
        elif "NUM_MESSAGES" in self.config:
            num_messages = self.config["NUM_MESSAGES"]
        else:
            num_messages = 1

        msjs = []
        for file in files:
            self.logger.debug(f"Reading File: {file}")
            with open(file, "rb") as f:
                avro_reader = fastavro.reader(f)
                data = avro_reader.next()
            if num_messages == 1:
                yield data
            else:
                msjs.append(data)
                if len(msjs) == num_messages:
                    return_msjs = msjs.copy()
                    msjs = []
                    yield return_msjs
