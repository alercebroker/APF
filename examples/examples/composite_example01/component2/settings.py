##################################################
#       component2   Settings File
##################################################

import pathlib

this_path = pathlib.Path(__file__).parent.resolve()
# Set the global logging level to debug
LOGGING_DEBUG = True

# Consumer configuration
CONSUMER_CONFIG = {
    "CLASS": "apf.consumers.csv.CSVConsumer",
    "FILE_PATH": this_path / "../data/inner_input.csv",
}

PRODUCER_CONFIG = {
    "CLASS": "apf.producers.csv.CSVProducer",
    "FILE_PATH": this_path / "../data/inner_data2.csv",
}
# Step Configuration
STEP_CONFIG = {
    "CONSUMER_CONFIG": CONSUMER_CONFIG,
    "PRODUCER_CONFIG": PRODUCER_CONFIG,
}
