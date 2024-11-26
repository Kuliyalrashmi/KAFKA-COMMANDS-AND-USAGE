# 1. Custom Partitioner: Handling Specific Keys

from kafka import KafkaProducer
import mmh3
import random


def custom_partitioner(key_bytes , all_partitions ,  available_partitions):
    if key_bytes is None:
        return available_partitions[0]

    key_str = key_bytes.decode('utf-8')

    if key_str == 'student':
        return all_partitions[-1] # send 'student' to last parititon

    return mmh3.hash(key_str) % len(all_partitions)

def custom_partitioner_to_handle_missing_keys(key_bytes , all_partitions ,  available_partitions):
    if key_bytes is None:
        return random.choice(available_partitions) # random partition is allocated
    
    return mm3.hash(key_bytes) % len(all_partitions)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    value_serializer=lambda v: v.encode('utf-8'),
    partitioner=custom_partitioner
)

producer2 = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    value_serializer=lambda v: v.encode('utf-8'),
    partitioner=custom_partitioner_to_handle_missing_keys
)


producer.send(topic = 'Records', key='student', value='Ram')  # Goes to the last partition
producer.send(topic = 'Records', key='teacher', value='Rakesh')
producer.send(topic = 'Records', key='teacher', value='Rani')
producer.send(topic = 'Records', key='police', value='Ramu')


producer2.send(topic = 'Records', value='Taimur')
producer2.send(topic = 'Records', value='Raka')
producer2.send(topic = 'Records', value='Ranjir')


producer.flush()
producer.close()

producer2.flush()
producer2.close()
