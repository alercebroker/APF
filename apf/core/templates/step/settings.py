##################################################
#       {{step_name}}   Settings File
##################################################

## Set the global logging level to debug
LOGGING_DEBUG = True

## Consumer configuration
CONSUMER_CONFIG = {
    "CLASS": "apf.consumers.generic.GenericConsumer",
}

## Step Configuration
STEP_CONFIG = {
    "CONSUMER_CONFIG": CONSUMER_CONFIG,
}
