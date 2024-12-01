from quixstreams import Application
import logging
import requests
import json

def GetData():

    url = "https://randomuser.me/api/"
    try: 
        data = requests.get(url)
        if data.status_code == 200:
            print(data.json())
            return data.json()
        
    except requests.ConnectionError as e:
        print(f"Error : {e}")
        return None

def main():
    # application is the entrypoint to to configure and interact with kafka topics
    app = Application(
        broker_address = "localhost:9092"
    )

    # defining a topic where message will be sent 
    topic = app.topic(name = "api_data" , value_serializer = 'json')

    data_records = GetData()
    
    if not data_records:
        return
    

    with app.get_producer() as producer:
        # it will convert message to correct format for kafka 
        kafka_message  = topic.serialize(key = data_records["results"][0]["login"]["uuid"] , value = data_records["results"][0])
        # now we send serialized message to kafka
        producer.produce(
            topic = topic.name,
            value = kafka_message.value,
            key =kafka_message.key
        )




    
if __name__ == "__main__":
    logging.basicConfig(level = "DEBUG")
    main()
