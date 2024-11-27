from kafka import KafkaConsumer
from kafka.errors import KafkaError
import struct
import logging

class Customer:
    def __init__(self, customer_id , customer_name):
        self.customer_id = customer_id
        self.customer_name = self.customer_name

    def __str__(self):
        return f"Customer Id : {self.customer_id} || Name : {self.customer_name}"



def custom_deserializer(value):
    if value is None:
        return None
    try:
        customer_id , name_size = struct.unpack("!ii" , value[:8])
        customer_name = value[8 : 8 + name_size].decode('utf-8')
        return Customer(customer_id, name)
    except Exception as e:
        print(f"Deserialization Error : {e}")
        return None

consumer = KafkaConsumer(
    'customerCountries',
    bootstrap_servers=['broker1:9092', 'broker2:9092'],
    group_id='CountryCounter',
    key_deserializer=lambda key: key.decode('utf-8') if key else None,
    value_deserializer=customer_deserializer
)

try:
    for message in consumer:
        customer = message.value
        if customer:
            print(f"Customer ID: {customer.customer_id}, Name: {customer.customer_name}")
except KafkaError as e:
    log.exception
finally:
    consumer.close()
