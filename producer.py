from time import sleep
from typing import Optional, Union
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import os
from typing import Iterator


old_topic = os.getenv("INPUT_TOPIC", "input_topic")
server = "kafka:9092"


def create_producer(server: str) -> KafkaProducer:
    """
    Create a Kafka producer

    Args:
        server (str): The host[:port] string of the broker

    Returns:
        KafkaProducer: The Kafka producer object
    """

    try:
        producer = KafkaProducer(
            bootstrap_servers=[server],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"))
    except NoBrokersAvailable as e:
        message = f"Failed to connect to broker. {e}"
        raise NoBrokersAvailable(message)
    
    except Exception as e:
        message = "Failed to create Kafka producer. {e}"
        raise Exception(message)
    
    print("Successfully created Kafka producer!")

    return producer


def read_messages(path: str) -> Iterator[dict]:
    """
    Read json messages from file into a list

    Args:
        path (str): The path to the json messages

    Returns:
        Iterator[dict]: A generator object of json messages
    """
    try:
        messages = (json.loads(line) for line in open(path, "r"))
    except Exception as e:
        message = f"Failed to read json from file. {e}"
        raise Exception(message)
    
    print("Successfully read json objects from file!")

    return messages


def send_messages(producer: KafkaProducer, topic: str, messages: Iterator[dict]):
    """
    Send messages to the provided Kafka topic

    Args:
        producer (KafkaProducer): The Kafka producer
        topic (str): The topic to send messages to 
        message_path (list): A path to the json messages
    """

    count = 0
    for message in messages:
        try:
            producer.send(topic, value=message)
            producer.flush()
            count += 1
        except Exception as e:
            message = f"Failed to send message to {topic}. {e}"
            raise Exception(message)
    
    print(f"Successfully sent {count} messages to {topic}!")
    

def main():
    producer = create_producer(server)
    messages = read_messages("messages.json")
    send_messages(producer, old_topic, messages)


if __name__ == "__main__":
    main()