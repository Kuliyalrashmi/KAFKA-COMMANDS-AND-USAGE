from kafka import KafkaProducer
from kafka.errors import KafkaError
from fastavro.schema import load_schema
from fastavro import writer
import io

class College_Student:
    def __init__(self , id ,  first_name , last_name , age , year , language_proficient , semester , email):
        self.id = id
        self.first_name = first_name
        self.last_name = last_name
        self.age = age 
        self.year = year
        self.language_proficient = language_proficient
        self.semester = semester
        self.email = email
    
    def to_dict():
        return{
            "id" : self.id,
            "first_name" : self.first_name,
            "last_name" : self.last_name,
            "age" : self.age,
            "year" : self.year,
            "langauge_used" : self.language_proficient,
            "semester" : self.semester,
            "email" : self.email
        }


schema = load_schema('student_record.avsc')

def avro_serializer(schema , data):
    bytes_writer = io.BytesIO()
    writer(bytes_writer , schema , [data])
    return bytes_writer.getvalue()

def main():
    producer = KafkaProducer(
        bootstrap_servers = ["localhost:9092"],
        retries = 5,
        max_block_ms = 2,
        values_serializer = lambda x : avro_serializer(schema , v),
        key_serializer = lambda l : l.encode('utf-8')
    )

    topic_name = "student_records"

    student1 = College_Student(id = 1 , first_name="ram" , last_name="kishan" , age=24 , year=4 , language_proficient= "java" , semester= 7 , email= "ramkishan54@gmail.com")

    future = producer.send(topic = topic_name , value= student1.to_dict())

    try:
        metadata = future.get(timeout = 10)
    except KafkaError as e:
        log.exception()
        pass

    print(f"Data written  to Topic : {metadata.topic} offset[{metadata.offset}] ")

    producer.flush()

    producer.close()


if __name__ == "__main__":
    main()
