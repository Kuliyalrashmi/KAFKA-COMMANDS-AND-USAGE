from kafka import KafkaConsumer
from kafka.error import KafkaError
import json


def commit_callback(err, partitions):
    """Callback function for handling commit responses."""
    if err:
        print(f"Commit failed: {err}")
    else:
        print(f"Commit succeeded for partitions: {partitions}")

def main():
    consumer = KafkaConsumer(
        "Records",
        group_id = "ConsumerGroup1",
        bootstrap_servers = ["localhost:9092"],
        value_deserializer = lambda value : value.decode('utf-8'),
        key_deserializer = lambda key : key.decode('utf-8'),
        enable_auto_commit = False,
    )

    consumer.subscribe(["Records"])

    try:
        while True:
            records = consumer.poll(timeout_ms= 1000)
            for partitions , messages in records:
                for message in messages :
                    print(f"Topic : {message.topic} || Partition : {message.partition}  || Offset : {message.offset}")
                    print(f"Key : {message.key} : Value {message.value} ")
            
            try : 
                future = consumer.commit_async(callback= commit_callback)
                if future:
                    future.get(timout = 10)
            except KafkaError as e:
                print(f"Error During Commit : {e}")
    
    except KeyboardInterrupt:
        print("Consumer Shutting Down !!")
    except KafkaError as e:
        print("Error : {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

# Be cautious about reprocessing duplicate messages during rebalancing since asynchronous commits may skip committing the latest processed offset.

# Retry Logic: Unlike commit_sync, commit_async doesn't retry failed commits. If you want retries, handle them explicitly in the callback function.

# Performance vs. Reliability: Commit less frequently for better throughput, but ensure your system can tolerate potential duplicates.
