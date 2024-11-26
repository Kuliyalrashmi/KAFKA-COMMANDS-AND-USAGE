from kafka import KafkaProducer
from kafka.errors import KafkaError
from fastavro.schema import load_schema
import io
from fastavro import writer
import random

schema = load_schema("C:/Users/ujjwa/Desktop/Kafka_series/learning_kafka/Kafka_Consumer/Custom_paritioners_Kafka/name_score.avsc")

def avro_serializer(data):
    bytes_writer = io.BytesIO()
    writer(bytes_writer , schema , [data])
    return bytes_writer.getvalue()


def success(metadata):
    print(f" Data Succesfuully written into  {metadata.topic} || partition : {metadata.partition} || offset : {metadata.offset} .")

def failure(excep):
    print(f"Cause  of failure operation : {excep}")

def InstantiaiteProducer():
    producer = KafkaProducer(
        bootstrap_servers = ['localhost:9092'],
        retries = 4,
        max_block_ms = 3000,
        batch_size = 32000,
        value_serializer = lambda value : avro_serializer(value),
        key_serializer = lambda key : key.encode('utf-8') if key is not None else b""
    )
    return producer

def main():

    producer = InstantiaiteProducer()
    topic_name = "StudentsScoreRecords"
    # case 1 : if key is not given then partition is done using round robin method

    data = [
        {"name": "Alice", "age": 25, "score": 88.5},
        {"name": "Bob", "age": 30, "score": 92.0}
    ]

    for record in data:
        future = producer.send(topic=topic_name, value=record)
        try:
            metadata = future.get(timeout=10)
            success(metadata)
        except Exception as e:
            print(f"Error: {e}")
            raise


    # case2 : if key is given it hashes based on key to ensure that all the data with same key should be put together..abs

    data2 = [
        {"name": "Alice", "age": 25, "score": 88.5},
        {"name": "Bob", "age": 30, "score": 92.0},
        {"name": "Charlie", "age": 22, "score": 78.0},
        {"name": "Diana", "age": 29, "score": 85.5},
        {"name": "Eve", "age": 35, "score": 91.0}
    ]



    for record  in data2 :
        key = random.choice(["record1", "record2"])
        producer.send(topic=topic_name, value=record, key=key).add_callback(success).add_errback(failure)


    producer.flush()
    producer.close()



if __name__ == "__main__":
    main()
    
    

