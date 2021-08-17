# Bot detection application.

---

## Environment

In this project I use following services:

- Zookeeper 
- Kafka
- Flume
- Spark Streaming
- Redis
- Cassandra

All environment runs in Docker. 
All images are setup in the docker-compose file int the recourses' folder.
To run this environment you should compile flume-filter application using
`mvn clean package` command. 

Now you could start all images using the `run-environment.sh` script. 
After that, in the docker, you could see the following services.

![img.png](screens/docker-env.png?raw=true "Docker")

---

##Kafka

Three Kafka brokers have been configured in the docker-compose file.
They have got external ports: 9092, 9093, 9094 and internal ports: 19092, 19093, 19094.
Other docker services must use internal ports to request Kafka.

Also, when the kafka is launched, a topic is created with the parameters:

- name: requests-data
- replication-factor: 3
- partitions: 6

---

##Flume

Flume service has build from the `flume.Dockerfile` in the resources' folder.
It runs at the 40000 port and has the following configurations:

![img.png](screens/flume-conf.png?raw=true "Flume")

It pulls directory with name `requests` and load all records to Kafka topic.
Also, this flume instance uses custom interceptor, that you should compile before running the environment script.
This interceptor filters json records and removes unnecessary symbols.
After that it uploads the required records to the Kafka topic.

---

##Redis

The Redis server is used to store suspicious bot IP addresses that Spark detects.
It runs at the 6379 port.

All bot IPs stores in the cache that called 'bots'. All records have
an expired policy and will be deleted after 10 minutes.

![img.png](screens/redis-recs.png?raw=true "Redis")

---

##Cassandra

Cassandra is used as a permanent database. 
All requests from Kafka stores in the Cassandra `botedetector` key space in the `bots` table.
Cassandra runs at the 9042 port.

---

## Logs generator

To generate logs data is used python log-generator.
To start the generating process you 
should run the `python3 logs-generator/logs-generator.py` command.

This command has the following arguments:

- --duration - time range of records in seconds.
- --users - count of 'user' records in the batch.
- --bots - count of 'bots' records in the batch.
- --freq - count of "user's" requests per second.
- --file - final file name.

---

## Spark Streaming

In this section Spark Streaming application find and filter
suspicious IPs using DStream and Structured Stream technologies.

It looks for ip addresses from which a large number of clicks were made over a certain period of time.
This IPs Spark stores to redis cache and after that filters all rows and mark bots requests as 'bot'.

Tests for DStream application:

![img.png](screens/DStreamTests.png?raw=true "Triggered dag")

Tests for Structured Streaming application:

![img.png](screens/struct-test.png?raw=true "Triggered dag")