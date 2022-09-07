import sys
sys.path.insert(0, ".")
from producer import create_producer, read_messages, send_messages
from transformer import create_consumer, consume_messages, change_timezone, transform_messages
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import pytest
import os
import subprocess
from typing import Iterator


SERVER = "localhost:29092"


class TestTransformer(object):

    def setup_class(self):
        command = f"docker exec local-kafka-broker kafka-topics --create --topic test_topic --bootstrap-server {SERVER}"
        subprocess.run(command, shell=True)

        producer = create_producer(SERVER)
        messages = ({f"a{i}": i} for i in range(5))
        send_messages(producer, "test_topic", messages)


    def teardown_class(self):
        command = f"docker exec local-kafka-broker kafka-topics --delete --topic test_topic --bootstrap-server {SERVER}"
        subprocess.run(command, shell=True)


    def test_create_consumer(self):
        consumer = create_consumer(SERVER, "test_topic", "test_group", 3)

        assert consumer is not None
        assert isinstance(consumer, KafkaConsumer)
    

    def test_create_consumer_exception(self):
        with pytest.raises(NoBrokersAvailable):
            consumer = create_consumer("localhost:9999", "test_topic", "test_group", 3)
    

    def test_create_consumer_bad_arguments(self):
        with pytest.raises(Exception):
            consumer = create_consumer(SERVER, 2, 1, "test_id")
    

    def test_consume_messages(self):
        consumer = create_consumer(SERVER, "test_topic", "test_group_pytest", 5)
        messages = consume_messages(consumer, "test_topic")

        assert isinstance(messages, Iterator)
        assert next(messages, None) is not None
        assert type(next(messages, None)) == dict
        assert next(messages, None) is not None
        assert next(messages, None) is not None
        assert next(messages, None) is not None
        assert next(messages, None) is None


    def test_change_timezone(self):
        assert type(change_timezone("2022-09-07T00:00:00")) == str
        assert change_timezone("2022-08-31T15:12:06+01:00") == "2022-08-31T14:12:06+00:00"
        assert change_timezone("2021-01-22T05:24:02+02:00") == "2021-01-22T03:24:02+00:00"

    
    def test_change_timezone_bad_argument(self):
        with pytest.raises(Exception):
            change_timezone("week")
        
        with pytest.raises(Exception):
            change_timezone("")
    

    def test_transform_messages(self):
        messages = [
            {"myKey": 1, "myTimestamp": "2022-03-01T09:11:04+01:00"},
            {"myKey": 2, "myTimestamp": "2022-03-01T09:12:08+01:00"}
        ]

        transformed_messages = [
            {"myKey": 1, "myTimestamp": "2022-03-01T08:11:04+00:00"},
            {"myKey": 2, "myTimestamp": "2022-03-01T08:12:08+00:00"}
        ]

        gen_messages = (message for message in messages)
        assert isinstance(transform_messages(gen_messages), Iterator)
        gen_messages = (message for message in messages)
        assert len(list(transform_messages(gen_messages))) == 2
        gen_messages = (message for message in messages)
        assert list(transform_messages(gen_messages)) == transformed_messages
    

    def test_transform_messages_missing_timestamp(self):
        messages = [
            {"myKey": 1, "myTimestamp": ""},
        ]

        transformed_messages = [
            {"myKey": 1, "myTimestamp": ""}
        ]

        gen_messages = (message for message in messages)
        assert isinstance(transform_messages(gen_messages), Iterator)
        gen_messages = (message for message in messages)
        assert len(list(transform_messages(gen_messages))) == 1
        gen_messages = (message for message in messages)
        assert list(transform_messages(gen_messages)) == transformed_messages


