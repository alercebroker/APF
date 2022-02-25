##################################################
#       lifecycle_example01   Settings File
##################################################

## Set the global logging level to debug
LOGGING_DEBUG = True

## Consumer configuration
CONSUMER_CONFIG = {}

## Step Configuration
STEP_CONFIG = {
    "CONSUMER_CONFIG": CONSUMER_CONFIG,
    "STEP_TYPE": "simple",
}
