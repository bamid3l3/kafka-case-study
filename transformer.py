import json
import datetime
from datetime import datetime
import pytz
import os
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from producer import create_producer, send_messages
from typing import Iterator


old_topic = os.getenv("INPUT_TOPIC", "input_topic")
new_topic = os.getenv("OUTPUT_TOPIC", "output_topic")
server = "kafka:9092"
group_id = os.getenv("CONSUMER_GROUP", "test_group")
timeout = int(os.getenv("CONSUMER_TIMEOUT", 3))


def create_consumer(server: str, topic: str, group_id: str, timeout: int) -> KafkaConsumer:
    """
    Create a Kafka consumer

    Args:
        server (str): The host[:port] string of the broker
        topic (str): The topic to which the consumer will be subscribed
        timout (int): Number of seconds before the consumer times out

    Returns:
        KafkaConsumer: The Kafka consumer object
    """
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[server],
            auto_offset_reset="earliest",
            group_id=group_id,
            enable_auto_commit=True,
            consumer_timeout_ms=timeout*1000,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")))
    except NoBrokersAvailable as e:
        message = f"Failed to connect to broker. {e}"
        raise NoBrokersAvailable(message)

    except Exception as e:
        message = f"Failed to create consumer. {e}"
        raise Exception(message)

    print("Successfully created consumer!")

    return consumer


def consume_messages(consumer: KafkaConsumer, topic: str) -> Iterator[dict]:
    """
    Consume all the messages from the provided topic into a list

    Args:
        consumer (KafkaConsumer): The Kafka consumer
        topic (str): The topic to consume messages from

    Returns:
        Generator[dict]: A generator object of the consumed messages
    """
    
    try:
        count = 0
        for message in consumer:
            message = message.value
            yield message
            count += 1
    except Exception as e:
        message = f"Failed to consume messages. {e}"
        raise Exception(message)

    print(f"Successfully consumed {count} from {topic}!")


def change_timezone(timestamp_string: str, new_timezone: str="UTC") -> str:
    """
    Change the timezone of a given timestamp string from the old timezone to the new timezone

    Args:
        timestamp_string (str):The timestamp string to change
        new_timezone (str, optional): The new timezone. Defaults to "UTC".

    Returns:
        str: The corrected timestamp string
    """

    new_timezone_object = pytz.timezone(new_timezone)

    old_timestamp = datetime.fromisoformat(timestamp_string)
    new_timestamp = old_timestamp.astimezone(new_timezone_object)
    new_timestamp_string = new_timestamp.isoformat()

    return new_timestamp_string


def transform_messages(messages: Iterator[dict]) -> Iterator[dict]:
    """
    Convert the timestamp in each message from Europe/Berlin to UTC

    Args:
        messages (Iterator[dict]): A generator of the messages to correct

    Returns:
        list: A generator of the corrected messages
    """
    
    count = 0
    for message in messages:
        if message["myTimestamp"] == "":
            yield message
            count += 1
            continue
        try:
            message["myTimestamp"] = change_timezone(message["myTimestamp"])
            yield message
            count += 1
        except Exception as e:
            error_message = f"Failed to process timestamp. {e}"
            raise Exception(error_message)
    
    print(f"Successfully transformed {count} messages!")

    return messages


def main():
    consumer = create_consumer(server, old_topic, group_id, timeout)
    messages = consume_messages(consumer, old_topic)
    transformed_messages = transform_messages(messages)
    producer = create_producer(server)
    send_messages(producer, new_topic, transformed_messages)


if __name__ == "__main__":
    main()
