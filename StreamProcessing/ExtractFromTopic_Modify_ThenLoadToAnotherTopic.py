from quixstreams import Application
import logging
from datetime import datetime
import json

def append_name(record):
    name = record["name"]
    return f"{name['title']}. {name['first']} {name['last']}"

def transform_record_location(record):
    curr_loc = record["location"]
    return (
        f"{curr_loc['country']} {curr_loc['state']} {curr_loc['city']}, "
        f"{curr_loc['street']['name']} {curr_loc['street']['number']} {curr_loc['postcode']}. "
        f"[{curr_loc['coordinates']['latitude']} {curr_loc['coordinates']['longitude']}]"
    )

def extract_date_of_birth(record):
    date = record["dob"]["date"]
    dt_object = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
    return dt_object.strftime("%d-%m-%Y %H:%M:%S")


def main():
    app = Application(
        broker_address="localhost:9092"
    )

    input_topic = app.topic(name="api_data", value_deserializer="json")
    output_topic = app.topic(name="processed_data", value_serializer="json")

    sdf = app.dataframe(input_topic)

    sdf["name"] = sdf.apply(lambda record: append_name(record))
    sdf["location"] = sdf.apply(lambda record: transform_record_location(record))
    sdf["dob"] = sdf.apply(lambda record: extract_date_of_birth(record))


    sdf = sdf.to_topic(output_topic)

    
    app.run()

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()
