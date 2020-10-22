import unittest
from unittest import mock
from apf.consumers import KafkaConsumer
from apf.consumers.kafka import datetime
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer
import os
import glob
import time

FILE_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLES_PATH = os.path.abspath(os.path.join(FILE_PATH, "../../examples/avro_test"))


class TestKafkaConsumer(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.client = AdminClient({"bootstrap.servers": "localhost:9094"})
        config = {"bootstrap.servers": "localhost:9094"}
        self.producer = Producer(config)
        self.topics = ["test"]

    def setUp(self):
        self.create_topics()
        self.produce_messages()
        self.config = {
            "TOPICS": self.topics,
            "PARAMS": {
                "bootstrap.servers": "localhost:9094",
                "group.id": "TestKafkaConsumer",
                "auto.offset.reset": "beginning",
                "enable.partition.eof": True,
            },
        }
        self.consumer = KafkaConsumer(self.config)

    def tearDown(self):
        self.consumer.consumer.close()
        self.clean_topics()
        del self.consumer

    def create_topics(self):
        new_topics = [NewTopic(topic, num_partitions=1) for topic in self.topics]
        fs = self.client.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")

    def produce_messages(self):
        try:
            for topic in self.topics:
                for data in self.read_avro():
                    self.producer.produce(topic, value=data)
                    self.producer.flush()
                print(f"produced to {topic}")
        except Exception as e:
            print(f"failed to produce to topic {topic}: {e}")

    def clean_topics(self):
        fs = self.client.delete_topics(self.topics)
        for topic, fs in fs.items():
            try:
                fs.result()
                print(f"topic {topic} deleted")
            except Exception as e:
                print(f"Failed to delete topic {topic}: {e}")

    def read_avro(self):
        files = glob.glob(os.path.join(EXAMPLES_PATH, "*.avro"))
        files.sort()
        self.nfiles = len(files)
        for f in files:
            with open(f, "rb") as fo:
                yield fo.read()

    def test_connection(self):
        topics = list(self.consumer.consumer.list_topics().topics.keys())
        for topic in self.topics:
            self.assertIn(topic, topics)

    def test_consume(self):
        count = 0
        for msg in self.consumer.consume():
            count += 1
        self.assertEqual(count, self.nfiles)

    def test_batch_consume(self):
        count = 0
        for msg in self.consumer.consume(num_messages=self.nfiles):
            count += 1
            self.assertIsInstance(msg, list)
            self.assertEqual(len(msg), self.nfiles)
        self.assertEqual(count, 1)

    def test_batch_config(self):
        self.consumer.consumer.close()
        del self.consumer
        self.config = {
            "TOPICS": self.topics,
            "PARAMS": {
                "bootstrap.servers": "localhost:9094",
                "group.id": "TestKafkaConsumerWithBatchConfig",
                "auto.offset.reset": "beginning",
                "enable.partition.eof": True,
            },
            "consume.messages": self.nfiles,
        }
        self.consumer = KafkaConsumer(self.config)
        count = 0
        for msg in self.consumer.consume(num_messages=self.nfiles):
            count += 1
            self.assertIsInstance(msg, list)
            self.assertEqual(len(msg), self.nfiles)
        self.assertEqual(count, 1)


class TestKafkaConsumerWithOffset(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.client = AdminClient({"bootstrap.servers": "localhost:9094"})
        config = {"bootstrap.servers": "localhost:9094"}
        self.producer = Producer(config)
        self.topics = ["test2"]

    def setUp(self):
        self.create_topics()
        self.produce_messages()
        # Get offset of the message in the middle
        # so that the first half is not consumed
        offset_start, offset_last = self.get_offsets()
        self.config = {
            "TOPICS": self.topics,
            "PARAMS": {
                "bootstrap.servers": "localhost:9094",
                "group.id": "TestKafkaConsumerWithOffset",
                "auto.offset.reset": "beginning",
                "enable.partition.eof": True,
            },
            "offset.init": offset_start,
            "offset.end": offset_last,
        }
        self.consumer = KafkaConsumer(self.config)

    def tearDown(self):
        self.consumer.consumer.close()
        self.clean_topics()
        del self.consumer

    def create_topics(self):
        new_topics = [NewTopic(topic, num_partitions=1) for topic in self.topics]
        fs = self.client.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")

    def produce_messages(self):
        try:
            for topic in self.topics:
                for data in self.read_avro():
                    self.producer.produce(topic, value=data)
                    time.sleep(1)
                    self.producer.flush()
                print(f"produced to {topic}")
        except Exception as e:
            print(f"failed to produce to topic {topic}: {e}")

    def clean_topics(self):
        fs = self.client.delete_topics(self.topics)
        for topic, fs in fs.items():
            try:
                fs.result()
                print(f"topic {topic} deleted")
            except Exception as e:
                print(f"Failed to delete topic {topic}: {e}")

    def read_avro(self):
        files = glob.glob(os.path.join(EXAMPLES_PATH, "*.avro"))
        files.sort()
        self.nfiles = len(files)
        for f in files:
            with open(f, "rb") as fo:
                yield fo.read()

    def get_offsets(self):
        config = {
            "bootstrap.servers": "localhost:9094",
            "group.id": "GetOffsetConsumer",
            "auto.offset.reset": "beginning",
            "enable.partition.eof": True,
        }
        consumer = Consumer(config)
        consumer.subscribe(self.topics)
        start = None
        end = None
        for i, msg in enumerate(consumer.consume(num_messages=self.nfiles)):
            if self.nfiles / 2 == i:
                start = datetime.datetime.fromtimestamp(msg.timestamp()[1] / 1000)
            if self.nfiles - 1 == i:
                end = datetime.datetime.fromtimestamp(msg.timestamp()[1] / 1000)
        return start.strftime("%d/%m/%Y %H:%M:%S"), end.strftime("%d/%m/%Y %H:%M:%S")

    def test_connection(self):
        self.consumer.consumer.poll()
        topics = list(self.consumer.consumer.list_topics().topics.keys())
        for topic in self.topics:
            self.assertIn(topic, topics)
        for topic in self.consumer.consumer.assignment():
            self.assertIn(topic.topic, self.topics)

    def test_consume_invalid_date(self):
        self.consumer.consumer.close()
        del self.consumer
        self.config = {
            "TOPICS": self.topics,
            "PARAMS": {
                "bootstrap.servers": "localhost:9094",
                "group.id": "TestKafkaConsumerWithOffsetsInvalidDate",
                "auto.offset.reset": "beginning",
                "enable.partition.eof": True,
            },
            "offset.init": "wrong_date",
            "offset.end": "wrong_date",
        }
        self.consumer = KafkaConsumer(self.config)
        count = 0
        for msg in self.consumer.consume():
            count += 1
        self.assertEqual(count, self.nfiles)

    def test_consume(self):
        # check that only half messages have timestamp after
        # the one set in config
        count = 0
        for msg in self.consumer.consume():
            count += 1
        self.assertEqual(count, self.nfiles // 2 - 1)
