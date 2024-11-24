from kafka import KafkaProducer
from kafka.errors import KafkaError

def async_call(data ,  topic_name):
    # instantiate producer object
    producer = KafkaProducer(
        bootstrap_servers = ['localhost:9092']
    )

    # use producer to send data
    future = producer.send(topic = topic_name , value = data.encode('utf-8'))

    # waiting for messages from client
    try:
        # if everything is correct metadata is sent back by broker
        metadata = future.get(timeout = 10)
    except KafkaError:
        log.exception()
        pass

    print("Producer Send data Successfully .")
    print("Topic name : ", metadata.topic)
    print("Partition : ",metadata.partition)
    print("Offset : ",metadata.offset)


    # ensure all message are sent
    producer.flush()

    # close producer
    producer.close()

    
def main():
    data = str(input("Enter data you want to sent to the topic : "))
    topic_name = str(input("Enter topic  name :"))

    async_call(data, topic_name)



if __name__ == "__main__":
    main()
