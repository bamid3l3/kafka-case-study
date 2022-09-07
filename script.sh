#!/usr/bin/env bash

set -o errexit; set -o pipefail; set -o nounset
umask 0022


# ------Set up variables--------
broker_port=""
zookeeper_port=""
input_topic=""
output_topic=""
consumer_group=""
consumer_timeout=""
test=""


while [[ $# > 0 ]]; do
  key=$1
  case $key in
    --broker-port) shift; broker_port=$1;;
    --zookeeper-port) shift; zookeeper_port=$1;;
    --input-topic) shift; input_topic=$1;;
    --output-topic) shift; output_topic=$1;;
    --consumer-group) shift; consumer_group=$1;;
    --consumer-timeout) shift; consumer_timeout=$1;;
    --test) shift; test=$1;;
    *) echo "Unknown argument provided: $1"; exit 1;;
  esac
  shift
done

if [[ $broker_port == "" ]]; then
  broker_port=29092
fi

if [[ $zookeeper_port == "" ]]; then
  zookeeper_port=22181
fi

if [[ $input_topic == "" ]]; then
  input_topic="input_topic"
fi

if [[ $output_topic == "" ]]; then
  output_topic="output_topic"
fi

if [[ $consumer_group == "" ]]; then
  consumer_group="test_group"
  exit 1
fi

if [[ $consumer_timeout == "" ]]; then
  consumer_timeout=5
fi

if [[ $test == "" ]]; then
  test="false"
fi

if [[ $test != "true" && $test != "false" ]]; then
    echo "Invalid value provided for the --test argument. Valid values are true or false"
    exit 1
fi

export BROKER_PORT=${broker_port}
export ZOO_PORT=${zookeeper_port}
export INPUT_TOPIC=${input_topic}
export OUTPUT_TOPIC=${output_topic}
export CONSUMER_GROUP=${consumer_group}
export CONSUMER_TIMEOUT=${consumer_timeout}

# printenv
# ------Set up infrastructure--------

echo "----Spinning up infrastructure----"

docker compose up -d
# docker compose convert
# exit 1

echo "----Successfully spun up infrastructure----"
echo ""

sleep 10

# ------Check that all services are running--------

service_count=$(docker ps | grep -c 'kafka-applications\|local-kafka-broker\|local-zookeeper')

if [ $service_count -ne 3 ]; then
    echo "All services are not running. Check that all requirements are met. Exiting..."
    exit 1
fi

echo "All three services are running..."
echo ""


if [[ $test == "true" ]]; then
    echo "-----Running unit tests----------"
    docker exec kafka-applications pytest
else
    # ------Create topics--------

    echo "----Creating topics-----"
    docker exec kafka-applications python topics.py


    # ------Check that topics were successfully created--------
    topic_count=$(docker exec local-kafka-broker kafka-topics --list --bootstrap-server localhost:${broker_port} \
                | grep -c "${input_topic}\|${output_topic}")

    if [ $topic_count -ne 2 ]; then
        echo "One or more topics not created. Check that all requirements are met. Exiting..."
        exit 1
    fi

    # ------Send messages--------
    echo ""
    echo "----Sending messages to input topic-----"
    docker exec kafka-applications python producer.py


    # ------Run transformer--------
    echo ""
    echo "----Running transformer application-----"
    docker exec kafka-applications python transformer.py
fi






