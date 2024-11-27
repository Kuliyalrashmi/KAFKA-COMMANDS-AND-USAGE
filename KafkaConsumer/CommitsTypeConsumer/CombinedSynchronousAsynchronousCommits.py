from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json

def main():
    consumer = KafkaConsumer(
        "Records",
        bootstrap_servers = ["localhost:9092"],
        group_id = "ConsumerGroup1",
        value_deserializer = lambda  value : value.decode('utf-8'),
        key_deserializer = lambda key : key.decode('utf-8')
    )

    consumer.subscribe(["Records"])

    try:
        while True:
    
            records = consumer.poll(timeout_ms= 1000)
            
            for partitions , messages  in records.items():
                for message in messages:
                    print(f"Topic : {message.topic} || partititon : {message.topic} || offset : {message.offset}")
                    print(f"key : {message.key} || value : {message.value}")
                
            consumer.commit_async(callback= commit_callback)

        
    except KeyboardInterrupt:
        print("Shutting Down Consumer  !!!")
    except KafkaError as e:
        print("Error : {e}")
    finally:
        try:
            consumer.commit()
            print("Synchronous commit completed before shutdown.")
        except KafkaError as e:
            print(f"Error during final synchronous commit: {e}")
        finally:
            consumer.close()


if __name__ == "__main__":
    main()