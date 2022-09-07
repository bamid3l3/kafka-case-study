#!/usr/bin/env bash
# Tears down the kafka infrastructure

set -o errexit; set -o pipefail; set -o nounset
umask 0022

retain=""

while [[ $# > 0 ]]; do
  key=$1
  case $key in
    --retain) shift; retain=$1;;
    *) echo "Unknown argument provided: $1. Confirm the usage"; exit 1;;
  esac
  shift
done

if [[ $retain == "" ]]; then
  retain="true"
  # exit 1
fi

if [[ $retain != "true" && $retain != "false" ]]; then
    echo "Invalid value provided for the --retain argument. Valid values are true or false"
    exit 1
fi

if [[ $retain == "true" ]]; then
    docker compose down
    echo "Successfully removed containers and network!"
else
    docker compose down --rmi all
    echo "Successfully removed containers, network, and images!"
fi