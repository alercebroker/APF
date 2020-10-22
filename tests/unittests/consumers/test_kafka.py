from .test_core import GenericConsumerTest
from apf.consumers import KafkaConsumer
from apf.consumers.kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import unittest
from unittest import mock
from confluent_kafka import Producer, TopicPartition
import datetime


class TestKafkaConsumer(GenericConsumerTest, unittest.TestCase):
    def setUp(self):
        self.component = KafkaConsumer
        self.params = {
            "TOPICS": ["apf_test"],
            "PARAMS": {"bootstrap.servers": "127.0.0.1:9092", "group.id": "apf_test"},
        }
        # sentinel allows for running consume loop a finite number of times
        sentinel = mock.PropertyMock(side_effect=[True, False, False])
        consumer_mock = mock.create_autospec(Consumer)
        self.consumer = self.component(self.params, consumer=consumer_mock)
        type(self.consumer).RUNNING = sentinel

    def tearDown(self):
        del self.consumer

    def test_no_topic(self):
        params = {
            "PARAMS": {"bootstrap.servers": "127.0.0.1:9092", "group.id": "apf_test"},
        }
        self.assertRaises(Exception, self.component, params)

    def test_num_messages_timeout(self):
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
            consumer = self.component(params, consumer=mock.create_autospec(Consumer))
            num, timeout = consumer.set_basic_config(1, 60)
            self.assertEqual(num, 100)
            self.assertEqual(timeout, 10)

    @mock.patch.object(KafkaConsumer, "_deserialize_message")
    def test_consume_no_error(self, mock_deserialize):
        mock_message = mock.Mock()
        mock_message.error.return_value = False
        self.consumer.consumer.consume.return_value = [mock_message]
        mock_deserialize.return_value = self.consumer.consumer.consume.return_value[0]
        for message in self.consumer.consume():
            self.assertEqual(message, mock_message)

    def test_consume_error(self):
        mock_message = mock.Mock()
        error_mock = mock.Mock()
        error_mock.name.return_value = "some_error"
        mock_message.error.return_value = error_mock
        self.consumer.consumer.consume.return_value = [mock_message]
        self.consumer.logger = mock.Mock()
        messages = self.consumer.consume()
        for message in self.consumer.consume():
            pass
        self.consumer.logger.exception.assert_called_with(
            f"Error in kafka stream: {error_mock}"
        )

    def test_consume_error_eof(self):
        mock_message = mock.Mock()
        error_mock = mock.Mock()
        error_mock.name.return_value = "_PARTITION_EOF"
        mock_message.error.return_value = error_mock
        self.consumer.consumer.consume.return_value = [mock_message]
        self.consumer.logger = mock.Mock()
        for message in self.consumer.consume():
            pass
        self.consumer.logger.info.assert_called_with("PARTITION_EOF: No more messages")

    @mock.patch.object(KafkaConsumer, "_deserialize_message")
    def test_batch_consume(self, mock_deserialize):
        mock_message = mock.Mock()
        mock_message.error.return_value = False
        self.consumer.consumer.consume.return_value = [mock_message, mock_message]
        mock_deserialize.return_value = mock_message
        for message in self.consumer.consume(num_messages=2):
            self.assertIsInstance(message, list)
            self.assertEqual(len(message), 2)

    @mock.patch.object(KafkaConsumer, "date_str_to_int")
    def test_init_with_timestamp(self, mock_get_date_int):
        config = {
            "TOPICS": ["apf_test"],
            "PARAMS": {
                "bootstrap.servers": "localhost:9092",
                "group.id": "test",
            },
            "offset.init": "some_datetime",
            "offset.end": "01/01/2000 00:00:00",
        }
        from apf.consumers.kafka import Consumer

        mock_consumer = mock.create_autospec(Consumer)
        mock_get_date_int.return_value = "start_date"
        self.consumer = self.component(config, consumer=mock_consumer)
        mock_get_date_int.assert_called()
        self.assertEqual(self.consumer.offsets["init"], "start_date")
        self.assertEqual(
            self.consumer.offsets["end"], datetime.datetime(2000, 1, 1, 0, 0)
        )

    @mock.patch.object(KafkaConsumer, "date_str_to_int")
    def test_on_assign_with_offsets(self, mock_date_str_to_int):
        config = {
            "TOPICS": ["apf_test"],
            "PARAMS": {
                "bootstrap.servers": "localhost:9092",
                "group.id": "test",
            },
            "offset.init": "some_datetime",
            "offset.end": "01/01/2000 00:00:00",
        }
        mock_consumer = mock.create_autospec(Consumer)
        self.consumer = self.component(config, consumer=mock_consumer)
        self.consumer.on_assign(mock_consumer, [mock.Mock()])
        mock_consumer.offsets_for_times.assert_called()


class TestKafkaConsumerDynamicTopic(unittest.TestCase):
    def setUp(self):
        self.component = KafkaConsumer
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
            "PARAMS": {"bootstrap.servers": "127.0.0.1:9092", "group.id": "apf_test"},
        }
        consumer_mock = mock.create_autospec(Consumer)
        # sentinel allows for running consume loop a finite number of times
        sentinel = mock.PropertyMock(side_effect=[True, False, False])
        self.consumer = self.component(self.params, consumer=consumer_mock)
        type(self.consumer).RUNNING = sentinel

    def tearDown(self):
        del self.consumer

    def test_recognizes_dynamic_topic(self):
        self.assertTrue(self.consumer.dynamic_topic)

    def test_creates_correct_topic_strategy_class(self):
        from apf.core.topic_management import DailyTopicStrategy

        self.assertTrue(isinstance(self.consumer.topic_strategy, DailyTopicStrategy))

    def test_subscribes_to_correct_topic_list(self):
        self.assertEqual(self.consumer.topics, [self.topic1, self.topic2])

    @mock.patch.object(KafkaConsumer, "_check_topics")
    @mock.patch.object(KafkaConsumer, "_subscribe_to_new_topics")
    def test_consume_no_change(self, mock_subscribe_new, mock_check_topics):
        mock_check_topics.return_value = False
        for message in self.consumer.consume():
            pass
        mock_check_topics.assert_called()
        mock_subscribe_new.assert_not_called()

    @mock.patch.object(KafkaConsumer, "_check_topics")
    @mock.patch.object(KafkaConsumer, "_subscribe_to_new_topics")
    def test_consume_change(self, mock_subscribe_new, mock_check_topics):
        mock_check_topics.return_value = True
        for message in self.consumer.consume():
            pass
        mock_check_topics.assert_called()
        mock_subscribe_new.assert_called()

    def test_check_topics_same(self):
        res = self.consumer._check_topics()
        self.assertFalse(res)

    def test_check_topics_change(self):
        mock_topic_strategy = mock.Mock()
        mock_topic_strategy.get_topics.return_value = ["something different"]
        self.consumer.topic_strategy = mock_topic_strategy
        res = self.consumer._check_topics()
        self.assertTrue(res)

    def test_subscribe_new_topics(self):
        self.consumer._subscribe_to_new_topics()
        self.consumer.consumer.unsubscribe.assert_called()
        self.consumer.consumer.subscribe.assert_called()
