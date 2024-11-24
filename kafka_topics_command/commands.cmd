// to create topic
bin\windows\kafka-topics --create --topic [topic_name] --bootstrap-server localhost:9092 --replication-factor [value] --partitions [value]

// list of all topics
bin\windows\kafka-topics --list --bootstrap-server localhost:9092

// to get description of a specific topic
bin\windows\kafka-topics --describe --topic [topic_name] --bootstrap-server localhost:9092

// to delete topic
bin\windows\kafka-topics --delete --topic [topic_name] --bootstrap-server localhost:9092

// alter topic
bin\windows\kafka-topics --alter --topic [topic_name] [--partitions [new_value] --replication-factor [new_value] ] --bootstrap-server localhost:9092


// to get more commands
bin\windows\kafka-topics --help
