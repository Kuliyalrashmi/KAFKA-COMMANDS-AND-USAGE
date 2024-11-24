from kafka import KafkaProducer
from kafka.errors import KafkaError
import json


class Customer:
    def __init__(self, name , id , age):
        self.name = name
        self.id = id
        self.age = age
    
    def to_dict(self):
        return {
            "id" : self.id,
            "name" : self.name,
            "age" : self.age,
        }

def success_callback(metadata):
    print(f"Data sent to topic: {metadata.topic}, partition: {metadata.partition}")

def error_callback(error):
    print(f"Error sending data: {error}")

def main():

    Producer = KafkaProducer(
        bootstrap_servers = ['localhost:9092'],
        retries = 4,
        acks = 1,
        batch_size = 32000,
        value_serializer = lambda  v : json.dumps(v).encode('utf-8')
    )

    obj = Customer("Ram" , 1 , 24) 
    topic_name = "customer_topic"
    
    try:
        Producer.send(topic = topic_name ,  value = obj.to_dict() ).add_callback(success_callback).add_errback(error_callback)

        Producer.flush()
    except KafkaError as e:
        print(f'Error Executing Serialization || Cause/Error : {e}'); 
    
    Producer.close()


if __name__ == "__main__":
    main()
