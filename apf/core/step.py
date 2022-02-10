from abc import abstractmethod

from apf.consumers import GenericConsumer
from apf.producers import GenericProducer
from apf.core import get_class
from apf.consumers import KafkaConsumer

import logging
import datetime


class GenericStep:
    """Generic Step for apf.

    Parameters
    ----------
    config : dict
        Dictionary containing configuration for the various components of the step

    level : logging.level
        Logging level, has to be a logging.LEVEL constant.

        Adding `LOGGING_DEBUG` to `settings.py` set the step's global logging level to debug.

        .. code-block:: python

            #settings.py
            LOGGING_DEBUG = True

    **step_args : dict
        Additional parameters for the step.
    """

    def __init__(
        self,
        config=None,
        level=logging.INFO,
        **step_args,
    ):
        step_types = ["simple", "composite", "component"]
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Creating {self.__class__.__name__}")
        self.config = config or {}
        self.consumer = self._get_consumer()(self.config["CONSUMER_CONFIG"])
        producer_config = self.config.get("PRODUCER_CONFIG") or {}
        self.producer = self._get_producer()(producer_config)
        self.step_type = self.config.get("STEP_TYPE", "simple")
        if self.step_type not in step_types:
            raise Exception(
                f"Step type can only be one of {step_types}. You provided {self.step_type}"
            )
        self.commit = self.config.get("COMMIT", True)
        self.metrics = {}
        self.metrics_sender = None
        self.extra_metrics = []
        if self.config.get("METRICS_CONFIG"):
            Metrics = get_class(
                self.config["METRICS_CONFIG"].get(
                    "CLASS", "apf.metrics.KafkaMetricsProducer"
                )
            )
            self.metrics_sender = Metrics(
                self.config["METRICS_CONFIG"]["PARAMS"]
            )
            self.extra_metrics = self.config["METRICS_CONFIG"].get(
                "EXTRA_METRICS", ["candid"]
            )

    def _get_consumer(self):
        if self.config.get("CONSUMER_CONFIG"):
            consumer_config = self.config["CONSUMER_CONFIG"]
            if "CLASS" in consumer_config:
                Consumer = get_class(consumer_config["CLASS"])
            else:
                Consumer = KafkaConsumer
            return Consumer
        raise Exception("Could not find CONSUMER_CONFIG in the step config")

    def _get_producer(self):
        if self.config.get("PRODUCER_CONFIG"):
            producer_config = self.config["PRODUCER_CONFIG"]
            if "CLASS" in producer_config:
                Consumer = get_class(producer_config["CLASS"])
            else:
                Consumer = GenericProducer
            return Consumer
        return GenericProducer

    def send_metrics(self, **metrics):
        """Send Metrics with a metrics producer.

        For this method to work the `METRICS_CONFIG` variable has to be set in the `STEP_CONFIG`
        variable.

        **Example:**

        Send the compute time for an object.

        .. code-block:: python

            #example_step/step.py
            self.send_metrics(compute_time=compute_time, oid=oid)

        For this to work we need to declare

        .. code-block:: python

            #settings.py
            STEP_CONFIG = {...
                "METRICS_CONFIG":{ #Can be a empty dictionary
                    "CLASS": "apf.metrics.KafkaMetricsProducer",
                    "PARAMS": { # params for the apf.metrics.KafkaMetricsProducer
                        "PARAMS":{
                            ## this producer uses confluent_kafka.Producer, so here we provide
                            ## arguments for that class, like bootstrap.servers
                            bootstrap.servers": "kafka1:9092",
                        },
                        "TOPIC": "metrics_topic" # the topic to store the metrics
                    },
                }
            }

        Parameters
        ----------
        **metrics : dict-like
            Parameters sent to the kafka topic as message.

        """
        if self.metrics_sender:
            metrics["source"] = self.__class__.__name__
            self.metrics_sender.send_metrics(metrics)

    def _pre_consume(self):
        self.logger.info("Starting step. Begin processing")
        self.pre_consume()

    @abstractmethod
    def pre_consume(self):
        pass

    def _pre_execute(self):
        self.logger.debug("Received message. Begin preprocessing")
        self.metrics["timestamp_received"] = datetime.datetime.now(
            datetime.timezone.utc
        )
        if self.step_type == "component" and self.commit:
            self.consumer.commit()
        self.pre_execute(self.message)

    @abstractmethod
    def pre_execute(self, message):
        pass

    @abstractmethod
    def execute(self, message):
        """Execute the logic of the step. This method has to be implemented by
        the instanced class.

        Parameters
        ----------
        message : dict, list
            Dict-like message to be processed or list of dict-like messages
        """
        pass

    def _post_execute(self, result):
        self.logger.debug("Processed message. Begin post processing")
        final_result = self.post_execute(result)
        if self.commit:
            self.consumer.commit()
        self.metrics["timestamp_sent"] = datetime.datetime.now(
            datetime.timezone.utc
        )
        time_difference = (
            self.metrics["timestamp_sent"] - self.metrics["timestamp_received"]
        )
        self.metrics["execution_time"] = time_difference.total_seconds()
        if self.extra_metrics:
            extra_metrics = self.get_extra_metrics(self.message)
            self.metrics.update(extra_metrics)
        self.send_metrics(**self.metrics)
        return final_result

    @abstractmethod
    def post_execute(self, result):
        pass

    def _pre_produce(self, result):
        self.logger.debug("Finished all processing. Begin message production")
        message_to_produce = self.pre_produce(result)
        return message_to_produce

    @abstractmethod
    def pre_produce(self, result):
        pass

    def _post_produce(self):
        self.logger.debug("Message produced. Begin post production")
        self.post_produce()

    @abstractmethod
    def post_produce(self):
        pass

    def get_value(self, message, params):
        """Get values from a massage and process it to create a new metric.

        Parameters
        ----------
        message : dict
            Dict-like message to be processed

        params : str, dict
            String of the value key or dict with the following:

            - 'key': str
                Must have parameter, has to be in the message.
            - 'alias': str
                New key returned, this can be used to standarize some message keys.
            - 'format': callable
                Function to be call on the message value.

        Returns
        -------
        new_key, value
            Aliased key and processed value.

        """
        if isinstance(params, str):
            return params, message.get(params)
        elif isinstance(params, dict):
            if "key" not in params:
                raise KeyError("'key' in parameteres not found")

            val = message.get(params["key"])
            if "format" in params:
                if not callable(params["format"]):
                    raise ValueError("'format' parameter must be a callable.")
                else:
                    val = params["format"](val)
            if "alias" in params:
                if isinstance(params["alias"], str):
                    return params["alias"], val
                else:
                    raise ValueError("'alias' parameter must be a string.")
            else:
                return params["key"], val

    def get_extra_metrics(self, message):
        """Generate extra metrics from the EXTRA_METRICS metrics configuration.

        Parameters
        ----------
        message : dict, list
            Dict-like message to be processed or list of dict-like messages

        Returns
        -------
        dict
            Dictionary with extra metrics from the messages.

        """
        # Is the message is a list then the metrics are
        # added to an array of values.
        if isinstance(message, list):
            extra_metrics = {}
            for msj in message:
                for metric in self.extra_metrics:
                    aliased_metric, value = self.get_value(msj, metric)
                    # Checking if the metric exists
                    if aliased_metric not in extra_metrics:
                        extra_metrics[aliased_metric] = []
                    extra_metrics[aliased_metric].append(value)
            extra_metrics["n_messages"] = len(message)

        # If not they are only added as a single value.
        else:
            extra_metrics = {}
            for metric in self.extra_metrics:
                aliased_metric, value = self.get_value(message, metric)
                extra_metrics[aliased_metric] = value
            extra_metrics["n_messages"] = 1
        return extra_metrics

    def start(self):
        """Start running the step."""
        self._pre_consume()
        for self.message in self.consumer.consume():
            self._pre_execute()
            result = self.execute(self.message)
            result = self._post_execute(result)
            result = self._pre_produce(result)
            self.producer.produce(result)
            self._post_produce()
        self._tear_down()

    def _tear_down(self):
        self.logger.info(
            "Processing finished. No more messages. Begin tear down."
        )
        self.tear_down()

    def tear_down(self):
        pass
