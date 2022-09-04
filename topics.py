from kafka.admin import KafkaAdminClient, NewTopic

server = "kafka:9092"

def instantiate_client(server: str, client_id: str) -> KafkaAdminClient:
    """
    Instantiate a client for administering a Kafka cluster

    Args:
        server (str): The host[:port] string of the broker
        client (str): A name for the client

    Returns:
        KafkaAdminClient: The kafka client
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=server, 
            client_id=client_id
        )
    except Exception as e:
        message = f"Error instantiating client. {e}"
        raise Exception(message)
    
    print("Successfully instantiated client!")

    return admin_client


def create_topic(admin_client: KafkaAdminClient, topic_name: str, partitions: int, replicas: int):
    """
    Create a kafka topic

    Args:
        topic_name (str): The name of the topic to create
        partitions (int): The number of partitions
        replicas (int): The number of replicas
    """
    try:
        topic = NewTopic(name=topic_name, num_partitions=partitions, replication_factor=replicas)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
    except Exception as e:
        message = f"Error creating topic. {e}"
        raise Exception(message)
    
    print(f"Successfully created {topic_name} topic!")


def main():
    client = instantiate_client(server, "test_client")
    create_topic(client, "input_topic", 1, 1)
    create_topic(client, "output_topic", 1, 1)


if __name__ == "__main__":
    main()