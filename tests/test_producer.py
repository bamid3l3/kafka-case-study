import sys
sys.path.insert(0, ".")
from producer import create_producer, read_messages, send_messages
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import pytest
import subprocess
import re
import tempfile
import os
import shutil
from typing import Iterator


SERVER = "localhost:29092"

class TestProducer(object):

    def setup_class(self):
        self.directory = tempfile.mkdtemp(dir=".")
        self.path = os.path.join(self.directory, "test.json")

        with open(self.path, "w") as f:
            f.write('{"a": 1}\n')
            f.write('{"b": 2}\n')
            f.write('{"c": 3}\n')
        
        # command = f"docker exec local-kafka-broker kafka-topics --create --topic test_topic --bootstrap-server {SERVER}"
        # subprocess.run(command, shell=True)


    def setup_method(self):
        command = f"docker exec local-kafka-broker kafka-topics --create --topic test_topic --bootstrap-server {SERVER}"
        subprocess.run(command, shell=True)


    def teardown_class(self):
        shutil.rmtree(self.directory)
        

    def teardown_method(self):        
        command = f"docker exec local-kafka-broker kafka-topics --delete --topic test_topic --bootstrap-server {SERVER}"
        subprocess.run(command, shell=True)


    def test_create_producer(self):
        producer = create_producer(SERVER)
        assert producer is not None
        assert isinstance(producer, KafkaProducer)
    

    def test_create_producer_exception(self):
        with pytest.raises(NoBrokersAvailable):
            producer = create_producer("localhost:9999")
        
    
    def test_read_messages(self):
        messages = read_messages(self.path)

        assert isinstance(messages, Iterator)
        assert next(messages, None) is not None
        assert type(next(messages, None)) == dict
        assert type(next(messages, None)) == dict
        assert next(messages, None) is None


    def test_send_messages(self):
        producer = create_producer(SERVER)
        messages = ({f"a{i}": i} for i in range(5))
        send_messages(producer, "test_topic", messages)

        command = "docker exec local-kafka-broker kafka-console-consumer --bootstrap-server localhost:29092 --topic test_topic --from-beginning --timeout-ms 5000 2>&1 | grep -c '{*}'"
        result = subprocess.run(command, shell=True, capture_output=True, text=True).stdout.strip()
        print(result)
        
        assert int(result) == 5
    
