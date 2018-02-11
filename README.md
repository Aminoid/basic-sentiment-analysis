## Basic Twitter Sentiment Analytics using Apache Spark Streaming APIs and Python

Processing live data streams using Spark’s streaming APIs and Kafka and performing a basic sentiment analysis of realtime tweets.

## Requirements
1. Run `sudo pip install - r requirements . txt`.
2. Download and extract the latest binary from https://kafka.apache.org/downloads.html.

## How to Run
1. Start zookeeper service
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
2. Start kafka service
```
bin/kafka-server-start.sh config/server.properties
```
3. Create a topic named twitterstream in kafka
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitterstream
```
4. Start Downloading tweets from the twitter stream API and push them to the twitterstream topic in Kafka
```
python twitter_to_kafka.py
```

5. Run the Stream Analysis Program
```
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.1 twitterStream.py
```
