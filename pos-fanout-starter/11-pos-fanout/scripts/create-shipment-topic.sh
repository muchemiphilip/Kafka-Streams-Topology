kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic shipment --config min.insync.replicas=2