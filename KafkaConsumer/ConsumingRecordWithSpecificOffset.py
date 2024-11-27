from kafka import KafkaConsumer , TopicPartition
import sqlite3

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    enable_auto_commit=False,
    auto_offset_reset='earliest'
)

db_connect = sqlite.connects("offsets.db")
cursor =  db_connect.cursor()

cursor.execute(
    """
        CREATE TABLE IF NOT EXISTS kafka_offsets (
            topic TEXT,
            partition INTEGER,
            offset INTEGER,
            PRIMARY KEY (topic, partition)
        )
    """
)

db_connect.commit()


def GetOffset(topic, partition):
    cursor.execute("""
        SELECT offset FROM kafka_offsets WHERE topic = ? AND partition = ?
    """, (topic, partition))
    result = cursor.fetchone()
    return result[0] if result else None

def store_offset(topic, partition, offset):
    """Store the offset in the database for a given partition."""
    cursor.execute("""
        INSERT OR REPLACE INTO kafka_offsets (topic, partition, offset)
        VALUES (?, ?, ?)
    """, (topic, partition, offset))
    db_connection.commit()

topic = 'my-topic'
partitions = consumer.partitions_for_topic(topic)
if partitions:
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)

    for tp in topic_partitions:
        stored_offset = GetOffset(tp.topic, tp.partition)
        if stored_offset is not None:
            consumer.seek(tp, stored_offset)
        else:
            print(f"No offset found for {tp}. Using default.")

try:
    while True:
        records = consumer.poll(timeout_ms=1000)
        for topic_partition, messages in records.items():
            for message in messages:
                print(f"Consumed message: topic={message.topic}, partition={message.partition}, "
                      f"offset={message.offset}, key={message.key}, value={message.value.decode('utf-8')}")

                store_offset(message.topic, message.partition, message.offset + 1)

except Exception as e:
    print(f"Error occurred: {e}")
finally:
    consumer.close()
    db_connection.close()
