cd /usr/local/kafka
sudo bin/zookeeper-server-start.sh config/zookeeper.properties
sudo bin/kafka-server-start.sh config/server.properties
sudo bin/kafka-console-producer.sh --topic spark --broker-list localhost:9092
sudo bin/kafka-console-consumer.sh --topic spark --from-beginning --bootstrap-server localhost:9092
