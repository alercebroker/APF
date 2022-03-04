##################################################
#       composite   Settings File
##################################################
import pathlib

this_path = pathlib.Path(__file__).parent.resolve()
# Set the global logging level to debug
LOGGING_DEBUG = True

# Consumer configuration
CONSUMER_CONFIG = {
    "INNER": {
        "CLASS": "apf.consumers.csv.CSVConsumer",
        "FILE_PATH": [
            this_path / "../data/inner_data1.csv",
            this_path / "../data/inner_data2.csv",
        ],
    },
    "OUTER": {
        "CLASS": "apf.consumers.csv.CSVConsumer",
        "FILE_PATH": this_path / "../data/input_data.csv",
    },
}

PRODUCER_CONFIG = {
    "INNER": {
        "CLASS": "apf.producers.csv.CSVProducer",
        "FILE_PATH": this_path / "../data/inner_input.csv",
    },
    "OUTER": {
        "CLASS": "apf.producers.csv.CSVProducer",
        "FILE_PATH": this_path / "../data/output.csv",
    },
}

# Step Configuration
STEP_CONFIG = {
    "CONSUMER_CONFIG": CONSUMER_CONFIG,
    "PRODUCER_CONFIG": PRODUCER_CONFIG,
}
