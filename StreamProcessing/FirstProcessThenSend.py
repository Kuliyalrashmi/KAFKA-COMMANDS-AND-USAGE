from quixstreams import Application
import json
import requests
import logging
import time

def GetData():
    url = "https://official-joke-api.appspot.com/random_joke"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
    except requests.ConnectionError as e:
        print(f"Connection Error : {e}")
        return None

def process_structure(record):
    return{
        "id" : record["id"],
        "user1" : record["setup"],
        "user2" : record["punchline"]
    }

    return record

def main():
    app = Application(
        broker_address = "localhost:9092"
    )

    output_topic = app.topic(name= "processed_data" , value_serializer = "json")

    try :
        with app.get_producer() as producer:
            while True:
                data = GetData()

                if not data:
                    continue

                processed_data = process_structure(data)

                kafka_message = output_topic.serialize(key =processed_data["id"] , value = processed_data)

                producer.produce(
                    topic = output_topic.name,
                    key = str(kafka_message.key),
                    value = kafka_message.value 
                )

                time.sleep(1)
    except Exception as e:
        print(f"Error : {e}")




if __name__ == "__main__":
    logging.basicConfig(level = "DEBUG")
    main()
