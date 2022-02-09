from .test_core import GenericConsumerTest
from apf.consumers import KafkaConsumer
import unittest
from unittest import mock
from confluent_kafka import KafkaException
from .kafka_message_mock import MessageMock
import datetime


class KafkaConsumerMock(unittest.TestCase):
    error = None

    def __init__(self, error=False, commit_error=False, max_retries=None):
        self.error = error
        self.commit_error = commit_error
        self.max_retries = max_retries
        if self.max_retries:
            self.retries = 0

    def subscribe(self, args):
        self.assertIsInstance(args, list)

    def consume(self, num_messages=1, timeout=60):
        messages = [MessageMock(self.error)] * num_messages
        return messages

    def commit(self, message=None, asynchronous=False):
        if self.max_retries:
            self.retries += 1
        if self.max_retries and self.retries == self.max_retries:
            return
        if self.commit_error:
            raise KafkaException()

        if message is not None:
            self.assertIsInstance(message, MessageMock)

    def unsubscribe(self):
        pass

    def close(self):
        pass


class TestKafkaConsumer(GenericConsumerTest, unittest.TestCase):
    component = KafkaConsumer
    params = {
        "TOPICS": ["apf_test"],
        "PARAMS": {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": "apf_test",
        },
    }

    @mock.patch(
        "apf.consumers.kafka.Consumer", return_value=KafkaConsumerMock()
    )
    def test_no_topic(self, mock_consumer):
        params = {
            "PARAMS": {
                "bootstrap.servers": "127.0.0.1:9092",
                "group.id": "apf_test",
            },
        }
        self.assertRaises(Exception, self.component, params)

    @mock.patch(
        "apf.consumers.kafka.Consumer", return_value=KafkaConsumerMock()
    )
    def test_num_messages_timeout(self, mock_consumer):
        opt_params = [
            {"consume.timeout": 10, "consume.messages": 100},
            {"TIMEOUT": 10, "NUM_MESSAGES": 100},
        ]
        for opt_param in opt_params:
            params = {
                "TOPICS": ["apf_test"],
                "PARAMS": {
                    "bootstrap.servers": "127.0.0.1:9092",
                    "group.id": "apf_test",
                },
            }
            params.update(opt_param)
            comp = self.component(params)
            for msj in comp.consume():
                self.assertIsInstance(msj, list)
                comp.commit()
                break

    @mock.patch(
        "apf.consumers.kafka.Consumer", return_value=KafkaConsumerMock()
    )
    def test_consume(self, mock_consumer):
        comp = self.component(self.params)
        for msj in comp.consume():
            self.assertIsInstance(msj, dict)
            comp.commit()
            break

    @mock.patch(
        "apf.consumers.kafka.Consumer", return_value=KafkaConsumerMock()
    )
    def test_batch_consume(self, mock_consumer):
        comp = self.component(self.params)
        for msj in comp.consume(num_messages=10, timeout=5):
            self.assertIsInstance(msj, list)
            comp.commit()
            break

    @mock.patch(
        "apf.consumers.kafka.Consumer",
        return_value=KafkaConsumerMock(error=True),
    )
    def test_consume_error(self, mock_consumer):
        comp = self.component(self.params)
        self.assertRaises(Exception, next, comp.consume())

    @mock.patch(
        "apf.consumers.kafka.Consumer",
        return_value=KafkaConsumerMock(commit_error=True),
    )
    def test_commit_error(self, mock_consumer):
        with self.assertRaises(KafkaException):
            comp = self.component(self.params)
            comp.commit()

    @mock.patch(
        "apf.consumers.kafka.Consumer",
        return_value=KafkaConsumerMock(commit_error=True, max_retries=2),
    )
    def test_commit_retry(self, mock_consumer):
        comp = self.component(self.params)
        comp.commit()


@mock.patch("apf.consumers.kafka.Consumer", return_value=KafkaConsumerMock())
class TestKafkaConsumerDynamicTopic(unittest.TestCase):
    component = KafkaConsumer
    now = datetime.datetime.utcnow()
    tomorrow = now + datetime.timedelta(days=1)
    date_format = "%Y%m%d"
    topic1 = "apf_test_" + now.strftime(date_format)
    topic2 = "apf_test_" + tomorrow.strftime(date_format)
    params = {
        "TOPIC_STRATEGY": {
            "CLASS": "apf.core.topic_management.DailyTopicStrategy",
            "PARAMS": {
                "topic_format": "apf_test_%s",
                "date_format": date_format,
                "change_hour": now.hour,
            },
        },
        "PARAMS": {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": "apf_test",
        },
    }

    def test_recognizes_dynamic_topic(self, mock_consumer):
        self.comp = self.component(self.params)
        self.assertTrue(self.comp.dynamic_topic)

    def test_creates_correct_topic_strategy_class(self, mock_consumer):
        from apf.core.topic_management import DailyTopicStrategy

        self.comp = self.component(self.params)
        self.assertTrue(
            isinstance(self.comp.topic_strategy, DailyTopicStrategy)
        )

    def test_subscribes_to_correct_topic_list(self, mock_consumer):
        self.comp = self.component(self.params)
        self.assertEqual(self.comp.topics, [self.topic1, self.topic2])

    def test_detects_new_topic_while_consuming(self, mock_consumer):
        import copy

        params = copy.deepcopy(self.params)
        np1 = self.now.hour + 1 if self.now.hour <= 24 else 0
        params["TOPIC_STRATEGY"]["PARAMS"]["change_hour"] = np1
        self.comp = self.component(params)
        self.comp.topic_strategy.change_hour = self.now.hour
        self.assertEqual(self.comp.topics, [self.topic1])
        for msg in self.comp.consume():
            self.comp.commit()
            break
        self.assertEqual(self.comp.topics, [self.topic1, self.topic2])
