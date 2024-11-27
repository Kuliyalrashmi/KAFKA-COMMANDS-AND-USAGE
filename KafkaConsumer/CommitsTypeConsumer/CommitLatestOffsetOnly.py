from kafka import KafkaConsumer
from kafka.errors import KafkaError, CommitFailedError
import json
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    consumer = KafkaConsumer(
        "Records", # topic_name
        group_id = "ConsumerGroup1",
        bootstrap_servers = ["localhost:9092"],
        value_deserializer = lambda value : value.decode('utf-8'),
        key_deserializer = lambda key : key.decode('utf-8'),
        enable_auto_commit = False
    )

    consumer.subscribe(["Records"]) # subscribe mulitple topics if i need

    try:
        records = consumer.poll(timeout_ms= 1000)
        for partitions , messages  in records.items():
            for message in  messages:
                print(f"Topic : {message.topic} || partition : {message.partition}  || Offset  : {message.offset}")
                print(f"Key : {message.key} , Value : {message.value}")

                custom_structure = {}
                custom_structure[message.key] = message.value
                print(json.dumps(custom_structure , indent =4))

            try:
                consumer.commit_sync()
                logger.info("Offsets successfully committed.")
            except CommitFailedError as e:
                logger.error("Offset commit failed", exc_info=e)

    except KeyboardInterrupt:
        print("Shutting Down Consumer...")
    except KafkaError as e :
        print(f"Error : {e}")
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()
