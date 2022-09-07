import sys
sys.path.insert(0, ".")
from topics import instantiate_client, create_topic
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import os
import pytest
import subprocess
import re


SERVER = "localhost:29092"

class TestClient(object):

    def test_instantiate_client(self):
        client = instantiate_client(SERVER, "test_id")
        
        assert client is not None
        assert isinstance(client, KafkaAdminClient)

    
    def test_instantiate_client_exception(self):
        with pytest.raises(NoBrokersAvailable):
            client = instantiate_client("localhost:9999", "test_id")


    def test_instantiate_client_no_exceptions(self):
        try:
            client = instantiate_client(SERVER, "test_id")
        except NoBrokersAvailable as e:
            assert False, e
    

    def test_instantiate_client_bad_arguments(self):
        with pytest.raises(Exception):
            client = instantiate_client(1, "test_id")
        with pytest.raises(Exception):
            client = instantiate_client("xx", "test_id")


class TestTopics(object):

    def setup_class(self):
        self.client = instantiate_client(SERVER, "test_id")

    def teardown_method(self):
        command = f"docker exec local-kafka-broker kafka-topics --delete --topic test_topic --bootstrap-server {SERVER}"
        subprocess.run(command, shell=True)
        

    def test_create_topic(self):
        create_topic(self.client, "test_topic", 1, 1)

        command = f"docker exec local-kafka-broker kafka-topics --list --bootstrap-server {SERVER} | grep -c 'test_topic'"
        
        result = subprocess.run(command, shell=True, capture_output=True, text=True).stdout.strip()
        assert int(result) == 1


    def test_create_topic_check_partitions_and_replicas(self):

        create_topic(self.client, "test_topic", 1, 1)
        command = f"docker exec local-kafka-broker kafka-topics --topic test_topic --describe --bootstrap-server {SERVER} | grep -Eo '(PartitionCount:\\s*\\d+)|(ReplicationFactor:\\s*\\d+)'"

        result = subprocess.run(command, shell=True, capture_output=True, text=True).stdout.strip()
        partition, replicas = [int(re.sub(r"[^\d]", "", r)) for r in result.split("\n")]

        assert partition == 1
        assert replicas == 1
    

    def test_create_topic_existing_topic(self):

        create_topic(self.client, "test_topic", 1, 1)
        
        try:
            create_topic(self.client, "test_topic", 1, 1)
        except Exception as e:
            assert False, e


        




    