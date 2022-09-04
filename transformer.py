import json
import datetime
from datetime import datetime
import pytz
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from producer import create_producer, send_messages

old_topic = "input_topic" 
new_topic = "output_topic"
server = "kafka:9092"


def create_consumer(server: str, topic: str) -> KafkaConsumer:
    """
    Create a Kafka consumer

    Args:
        server (str): The host[:port] string of the broker
        topic (str): The topic to which the consumer will be subscribed

    Returns:
        KafkaConsumer: The Kafka consumer object
    """
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[server],
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")))
    except Exception as e:
        message = f"Failed to create consumer. {e}"
        raise Exception(message)

    print("Successfully created consumer!")

    return consumer


def consume_messages(consumer: KafkaConsumer, topic: str) -> list:
    """
    Consume all the messages from the provided topic into a list

    Args:
        consumer (KafkaConsumer): The Kafka consumer
        topic (str): The topic to consume messages from

    Returns:
        list: A list of the consumed messages
    """

    topic_partition = TopicPartition(topic, 0)
    last_offset = consumer.end_offsets([topic_partition])[topic_partition]

    messages = []
    
    try:
        for message in consumer:
            message_offset = message.offset
            message = message.value
            messages.append(message)
            if message_offset == last_offset - 1:
                break
    except Exception as e:
        message = f"Failed to consume messages. {e}"
        raise Exception(message)

    print(f"Successfully consumed {len(messages)} from {topic}!")

    return messages


def change_timezone(timestamp_string: str, old_timezone: str="Europe/Berlin", new_timezone: str="UTC") -> str:
    """
    Change the timezone of a given timestamp string from the old timezone to the new timezone

    Args:
        timestamp_string (str):The timestamp string to change
        old_timezone (str, optional): The old timezone. Defaults to "Europe/Berlin".
        new_timezone (str, optional): The new timezone. Defaults to "UTC".

    Returns:
        str: The corrected timestamp string
    """

    old_timezone_object = pytz.timezone(old_timezone)
    new_timezone_object = pytz.timezone(new_timezone)

    old_timestamp = datetime.fromisoformat(timestamp_string).replace(tzinfo=None)
    localized_timestamp = old_timezone_object.localize(old_timestamp)
    new_timestamp = localized_timestamp.astimezone(new_timezone_object)
    new_timestamp_string = new_timestamp.isoformat()

    return new_timestamp_string


def transform_messages(messages: list) -> list:
    """
    Convert the timestamp in each message from Europe/Berlin to UTC

    Args:
        messages (list): The list of messages to correct

    Returns:
        list: The list of corrected messages
    """
    
    for message in messages:
        if message["myTimestamp"] == "":
            continue
        try:
            message["myTimestamp"] = change_timezone(message["myTimestamp"])
        except Exception as e:
            error_message = f"Failed to process timestamp. {e}"
            raise Exception(error_message)
    
    print("Successfully transformed messages!")

    return messages


def main():
    consumer = create_consumer(server, old_topic)
    messages = consume_messages(consumer, old_topic)
    transformed_messages = transform_messages(messages)
    producer = create_producer(server)
    send_messages(producer, new_topic, transformed_messages)


if __name__ == "__main__":
    main()
