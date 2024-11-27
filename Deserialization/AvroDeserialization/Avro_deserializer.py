from kafka import KafkaConsumer
import fastavro
import io
import json
from fastavro.schema import load_schema
from fastavro import reader

def load_schem():
    schema = load_schema("C:/Users/ujjwa/Desktop/Kafka_series/learning_kafka/Kafka_Consumer/Deserializers/AvroDeserialization/customer.avsc")
    return schema

def avro_deserialzer(data):
    if value  is None:
        return None
    bytes_reader = io.BytesIO(data)
    return next(fastavro.reader(bytes_reader, schema))


def main():
    producer = KafkaConsumer(
        'customer_topic',
        bootstrap_servers=['broker1:9092', 'broker2:9092'],
        group_id='customer_group',
        value_deserializer=lambda v: avro_deserializer(v, schema)
    )

    for message in consumer:
        customer = message.value
        if customer:
            print(f"Customer ID: {customer['customerID']}, Name: {customer['customerName']}")
