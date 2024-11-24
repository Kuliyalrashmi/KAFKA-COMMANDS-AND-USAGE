from kafka import KafkaProducer
from kafka.errors import KafkaError

def main():
    producer = KafkaProducer(
        bootstrap_servers = ['localhost:9092'],
        acks =  0,
        retries = 5,
        linger_ms = 5,
        batch_size = 32000,
        compression_type = 'gzip',
        client_id = 'python-producer-1'
    )


    # config2 = {
    #     'bootstrap_servers' : ['localhost:9092'],
    #     'acks' : -1, # all
    #     'retries' : 4,
    #     'linger.ms' : 5,
    #     'compression.type' : 'gzip',
    #     "client.id" : 'python_producer_2'
    # }

    # producer2 = KafkaProducer(config2)

    def delivery_report(err, msg):
        if err:
            print(f"message delivery failed due to : {err}")
        else:
            print(f"message successfully delivered to {msg.topic} [{msg.partition}]")
        
    producer.produce(topic = topic_name , value = data.encode('utf-8') , callback = delivery_report)

    producer.flush()

    producer.close()


if __name__ == "__main__":
    main()
    
