from .test_core import GenericConsumerTest
from apf.consumers.kafka import KafkaJsonConsumer, KafkaConsumer, KafkaSchemalessConsumer
import unittest
from unittest import mock
from confluent_kafka import KafkaException
from .message_mock import MessageMock, MessageJsonMock, SchemalessMessageMock, SchemalessBadMessageMock
import datetime
import os


def consume(num_messages=1, message_mock=MessageMock):
    messages = [[message_mock(False)] * num_messages]
    messages.append([message_mock(True)])
    return messages

@mock.patch("apf.consumers.kafka.Consumer")
class TestKafkaConsumer(GenericConsumerTest):
    def setUp(self) -> None:
        self.params = {
            "TOPICS": ["apf_test"],
            "PARAMS": {
                "bootstrap.servers": "127.0.0.1:9092",
                "group.id": "apf_test",
            },
        }
        self.component = KafkaConsumer(self.params)

    def test_no_topic(self, _):
        params = {
            "PARAMS": {"bootstrap.servers": "127.0.0.1:9092", "group.id": "apf_test"},
        }

        def initialize_consumer(params):
            self.component = KafkaConsumer(params)

        self.assertRaises(Exception, initialize_consumer, params)

    def test_num_messages_timeout(self, mock_consumer):
        mock_consumer().consume.side_effect = consume(num_messages=10)
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
            self.component = KafkaConsumer(params)
            for msj in self.component.consume():
                self.assertIsInstance(msj, list)
                self.assertEqual(len(msj), 10)
                break

    def test_consume(self, mock_consumer):
        self.component = KafkaConsumer(self.params)
        mock_consumer().consume.side_effect = consume(num_messages=1)
        for msj in self.component.consume():
            self.assertIsInstance(msj, dict)
            break

    def test_batch_consume(self, mock_consumer):
        self.component = KafkaConsumer(self.params)
        mock_consumer().consume.side_effect = consume(num_messages=1)
        for msj in self.component.consume(num_messages=10, timeout=5):
            self.assertIsInstance(msj, list)
            # should be equal to available messages even if num_messages is higher
            self.assertEqual(len(msj), 1)
            break

    def test_consume_error(self, mock_consumer):
        self.component = KafkaConsumer(self.params)
        mock_consumer().consume.side_effect = consume(num_messages=0)
        self.assertRaises(Exception, next, self.component.consume())

    def test_commit_error(self, mock_consumer):
        self.component = KafkaConsumer(self.params)
        mock_consumer().commit.side_effect = KafkaException
        with self.assertRaises(KafkaException):
            self.component.commit()

    def test_commit_retry(self, mock_consumer):
        self.component = KafkaConsumer(self.params)
        mock_consumer().commit.side_effect = ([KafkaException] * 4) + [None]
        self.component.commit()


@mock.patch("apf.consumers.kafka.Consumer")
class TestKafkaConsumerDynamicTopic(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime.datetime.utcnow()
        self.tomorrow = self.now + datetime.timedelta(days=1)
        self.date_format = "%Y%m%d"
        self.topic1 = "apf_test_" + self.now.strftime(self.date_format)
        self.topic2 = "apf_test_" + self.tomorrow.strftime(self.date_format)
        self.params = {
            "TOPIC_STRATEGY": {
                "CLASS": "apf.core.topic_management.DailyTopicStrategy",
                "PARAMS": {
                    "topic_format": "apf_test_%s",
                    "date_format": self.date_format,
                    "change_hour": self.now.hour,
                },
            },
            "PARAMS": {
                "bootstrap.servers": "127.0.0.1:9092",
                "group.id": "apf_test",
            },
        }
        self.component = KafkaConsumer(self.params)

    def test_recognizes_dynamic_topic(self, mock_consumer):
        self.component = KafkaConsumer(self.params)
        self.assertTrue(self.component.dynamic_topic)

    def test_creates_correct_topic_strategy_class(self, mock_consumer):
        from apf.core.topic_management import DailyTopicStrategy

        self.component = KafkaConsumer(self.params)
        self.assertTrue(
            isinstance(
                self.component.topic_strategy,
                DailyTopicStrategy,
            )
        )

    def test_subscribes_to_correct_topic_list(self, mock_consumer):
        self.component = KafkaConsumer(self.params)
        self.assertEqual(self.component.topics, [self.topic1, self.topic2])

    def test_detects_new_topic_while_consuming(self, mock_consumer):
        import copy

        mock_consumer().consume.side_effect = consume(num_messages=2)
        params = copy.deepcopy(self.params)
        np1 = self.now.hour + 1 if self.now.hour <= 24 else 0
        params["TOPIC_STRATEGY"]["PARAMS"]["change_hour"] = np1
        self.component = KafkaConsumer(params)
        self.component.topic_strategy.change_hour = self.now.hour
        self.assertEqual(self.component.topics, [self.topic1])
        for _ in self.component.consume():
            self.component.commit()
            break
        self.assertEqual(self.component.topics, [self.topic1, self.topic2])


class TestKafkaJsonConsumer(unittest.TestCase):
    component = KafkaJsonConsumer
    params = {
        "TOPICS": ["apf_test"],
        "PARAMS": {"bootstrap.servers": "127.0.0.1:9092", "group.id": "apf_test"},
    }

    def test_deserialize(self):
        msg = MessageJsonMock()
        consumer = self.component(self.params)
        consumer._deserialize_message(msg)


class TestKafkaSchemalessConsumer(unittest.TestCase):

    FILE_PATH = os.path.dirname(__file__)
    SCHEMALESS_CONSUMER_SCHEMA_PATH = os.path.join(FILE_PATH, "../examples/kafka_schemalessconsumer_schema.avsc")
    SCHEMALESS_CONSUMER_BAD_SCHEMA_PATH =  os.path.join(FILE_PATH, "../examples/kafka_schemalessconsumer_bad_schema.avsc")

    def test_schema_no_path(self):
        params = {
            "TOPICS": ["apf_test"],
            "PARAMS": {
                "bootstrap.servers": "127.0.0.1:9092",
                "group.id": "apf_test",
            },
        }
        with self.assertRaises(Exception):
            KafkaSchemalessConsumer(params)

    def test_shcema_path_to_bad_file(self):
        params = {
            "TOPICS": ["apf_test"],
            "PARAMS": {
                "bootstrap.servers": "127.0.0.1:9092",
                "group.id": "apf_test",
            },
            "SCHEMA_PATH": self.SCHEMALESS_CONSUMER_BAD_SCHEMA_PATH
        }
        with self.assertRaises(Exception):
            KafkaSchemalessConsumer(params)

    def test_schemaless_deserialize(self):
        schemaless_avro = SchemalessMessageMock(False)
        expected_message = {"key": "llave", "value": 1}

        params = {
            "TOPICS": ["apf_test"],
            "PARAMS": {
                "bootstrap.servers": "127.0.0.1:9092",
                "group.id": "apf_test",
            },
            "SCHEMA_PATH": self.SCHEMALESS_CONSUMER_SCHEMA_PATH
        }

        consumer = KafkaSchemalessConsumer(params)

        result = consumer._deserialize_message(schemaless_avro)

        self.assertDictEqual(result, expected_message)

    def test_schemaless_deserialize_bad_message(self):
        schemaless_avro = SchemalessBadMessageMock(False)

        params = {
            "TOPICS": ["apf_test"],
            "PARAMS": {
                "bootstrap.servers": "127.0.0.1:9092",
                "group.id": "apf_test",
            },
            "SCHEMA_PATH": self.SCHEMALESS_CONSUMER_SCHEMA_PATH
        }

        consumer = KafkaSchemalessConsumer(params)

        with self.assertRaises(Exception):
            consumer._deserialize_message(schemaless_avro)

    @mock.patch("apf.consumers.kafka.Consumer")
    def test_batch_consume(self, mock_consumer):
        params = {
            "TOPICS": ["apf_test"],
            "PARAMS": {
                "bootstrap.servers": "127.0.0.1:9092",
                "group.id": "apf_test",
            },
            "SCHEMA_PATH": self.SCHEMALESS_CONSUMER_SCHEMA_PATH
        }
        component = KafkaSchemalessConsumer(params)
        mock_consumer().consume.side_effect = consume(num_messages=9, message_mock=SchemalessMessageMock)
        for msj in component.consume(num_messages=10, timeout=5):
            self.assertIsInstance(msj, list)
            # should be equal to available messages even if num_messages is higher
            self.assertEqual(len(msj), 9)
            break
