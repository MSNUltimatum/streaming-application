#!/bin/bash

docker build -f resources/flume.Dockerfile -t myflume .
# shellcheck disable=SC2164
cd resources
docker-compose up -d zookeeper kafka1 kafka2 kafka3 redis redis-ui cassandra
sleep 15
docker-compose up -d flume
docker cp cassandra.cql cassandra:/
docker exec -d cassandra cqlsh -u cassandra -p cassandra -f cassandra.cql