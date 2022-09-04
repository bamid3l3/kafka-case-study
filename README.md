# **Kafka Case Study**
<br/>


## **Overview**  

*Problem*: An exisitng Kafka topic is discovered to contain malformed data. Upon discovery of this problem, the ingestion into the topic is halted. The wrong data already in the kafka topic has to be corrected and ingested into a new topic.  
  
This repo provides a POC solution that involves setting up a local kafka cluster with a single broker for development. Two topics (`input_topic` and `output_topic`) are created by a simple client application (`topics.py`) and `input_topic` is populated with the malformed data in the `messages.json` file by a producer application (`producer.py`). A transformer application (`transformer.py`) consumes the malformed messages from `input_topic`, transforms/corrects the messages and sends them to `output_topic`. The transformation involves converting the timezone of the timestamp value in each message from Europe/Berlin to UTC.

<br/>
<br/>

## **Prerequisites**
To reproduce this application in your environment, the installation of **Docker** and **docker-compose** is required. For steps on how to install Docker, visit this [link](https://docs.docker.com/engine/install/).

<br/>
<br/>


## **Usage**
The step-by-step instructions on how to reproduce this solution are given below:
- `cd` into the root of the project and ensure that your working directory has the `docker-compose.yml` in it.


- Spin up the infrastructure including the broker, zookeeper, and the python applications with the command below (Ensure that the ports **29092** and **22181** are open on your machine):
                            
    ```sh
    docker-compose up -d
    ```


    N.B.: The kafka broker is exposed to the host machine via [localhost:29092](localhost:29092). This will be useful if you want to connect to the cluster using a GUI tool like offset explorer (on mac) or any other client applications.


- Confirm that all three services are running:

    ```sh
    docker ps
    ```

    Three containers with the names **kafka-applications**, **local-kafka-broker**, and **local-zookeeper** should be running.  



- Create the topics in the cluster

    ```sh
    docker exec kafka-applications python topics.py
    ```

    To check that the topics were successfully created:

    ```sh
    docker exec local-kafka-broker kafka-topics --list --bootstrap-server localhost:29092
    ```



- Populate `input_topic` with the malformed data

    ```sh
    docker exec kafka-applications python producer.py
    ```



- Consume the messages from `input_topic`, correct the messages, and send the corrected messages to `output_topic`

    ```sh
    docker exec kafka-applications python transformer.py
    ```


- (Optional) Display the messages in each topic

    ```sh
    docker exec local-kafka-broker kafka-console-consumer --bootstrap-server localhost:29092 --topic input_topic --from-beginning --max-messages 5
    ```

    ```sh
    docker exec local-kafka-broker kafka-console-consumer --bootstrap-server localhost:29092 --topic output_topic --from-beginning --max-messages 5
    ```


- Finally, stop and remove the running containers and network

    ```sh
    docker compose down
    ```

    To completely tear down the application (including the docker images):

    ```sh
    docker compose down --rmi all
    ```






