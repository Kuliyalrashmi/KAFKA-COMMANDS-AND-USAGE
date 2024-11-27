from kafka import KafkaProducer
import fastavro
import io
import json
from fastavro.schema import load_schema
from fastavro import writer

def load_schem():
    schema = load_schema("C:/Users/ujjwa/Desktop/Kafka_series/learning_kafka/Kafka_Consumer/Deserializers/AvroDeserialization/customer.avsc")

def avro_serialzer(data):
    bytes_writer = io.BytesIO()
    schema = load_schem()
    writer(bytes_writer , schema , [data])
    return bytes_writer.getvalue()


def success(metadata):
    print(f"Data Successfully written to {metadata.topic} || partition : {metadata.partition} || offset  : {metadata.offset} .")


def error(excep):
    print(f"Error while writting data due to {excep}")


def main():
    producer = KafkaProducer(
        bootstrap_servers = ["localhost:9092"],
        value_serializer = lambda value : value.avro_serialzer(value),
        key_serializer = lambda key : key.encode('utf-8'),
        batch_size = 32000,
        max_block_ms = 1000
    )

    topic_name = 'customer_topic'
    data = {"customerID": 123, "customerName": "John Doe"}

    try:
        producer.send(topic = topic_name ,  key  = data ).add_callback(success).call_errback(error)
    except KeyboardInterrupt:
        print("Shutting Down Consumer !!!")
    except KafkaError as e:
        print(f"Kafka Error : {e}")
    finally:
        consumer.flush()
        consumer.close()
