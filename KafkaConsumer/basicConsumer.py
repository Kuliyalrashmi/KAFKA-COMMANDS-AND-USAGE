from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json

def main():
    # instantiating consumer object 
    consumer = KafkaConsumer(
        "Records", #topic_name
        group_id = "ConsumerGroup1",
        bootstrap_servers = ['localhost:9092'],
        value_deserializer = lambda value : value.decode('utf-8'),
        key_deserializer = lambda key : key.decode('utf-8') if key else None
    )

    # subscribing to topics
    consumer.subscribe(['Records'])

    # polling for data :
    try : 
        while True:
            records = consumer.poll(timeout_ms = 100)

            for topic_partition , messages in records.items():
                for message in messages:
                    print(f"Topic : {message.topic} , partition : {message.partition} , offset : {message.offset}")
                    print(f"Key : {message.key} , Value : {message.value}")

                    # we can use a dictionary to store data in some structured way
                    custom_map = {}
                    custom_map[message.key] = message.value
                    print(json.dumps(custom_map, indent=4))

    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    except KafkaError as e:
        print(f"Kafka error occurred: {e}")
    finally:
        consumer.close()     



if __name__ == "__main__":
    main()
