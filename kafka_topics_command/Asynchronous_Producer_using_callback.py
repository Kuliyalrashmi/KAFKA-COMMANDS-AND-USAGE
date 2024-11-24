from kafka import KafkaProducer
from kafka.errors import KafkaError

def call_asynchronous_producer(topic_name , data):
    # instantiate producer object
    producer = KafkaProducer(
        bootstrap_servers = ['localhost9092']
    )

    # create a success callback (i want info of metadata for each success)
    def on_success(record_metadata):
        print("Topic : ",record_metadata.topic)
        print("Offset : ",record_metadata.offset)
        print("Partition Count : ", record_metadata.partition)
    
    def on_error(excp):
        log.error('I am an errback', exc_info=excp)
        # further logic to handle exception

    producer.send(topic = topic_name , value= data.encode('utf-8')).add_callback(on_success).add_errback(on_error)

    # flush all the messages
    producer.flush()

    # close the producer
    producer.close()

def main():
    topic_name = str(input("Enter Topic Name : "))
    data = str(input(f"Enter data you want to send to topic {topic_name} : "))
    call_asynchronous_producer(topic_name , data)



if __name__ == "__main__":
    main()
