from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json

def main():
    consumer = KafkaConsumer(
        "Records", # topic_name
        group_id = "ConsumerGroup1",
        bootstrap_servers = ["localhost:9092"],
        value_deserializer = lambda value : value.decode('utf-8'),
        key_deserializer = lambda key : key.decode('utf-8'),
        enable_auto_commit = True
    )

    consumer.subscribe(["Records"]) # subscribe mulitple topics if i need

    try:
        records = consumer.poll(timeout_ms= 1000)
        for partitions , messages  in records.items():
            for message in  messages:
                print(f"Topic : {message.topic} || partition : {message.partition}  || Offset  : {message.offset}")
                print(f"Key : {message.key} , Value : {message.value}")

                custom_structure = {}
                custom_structure[message.key] = message.value
                print(json.dumps(custom_structure , indent =4))
    
    except KeyboardInterrupt:
        print("Shutting Down Consumer...")
    except KafkaError as e :
        print(f"Error : {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()



# problem kya  he auto commit ke sath

# kafka me consumers ko pta hota he ki unka starting point khan pr he (because of the concept of commit) . One of them is auto commit jhan pr consumer khud offset commit krta after complete processing of offset.

# Scenario where auto commit cause problem:  suppose default interval commit ka 5 ms he but agr after 3 ms rebalancing hui so due to this rebalancing current state dismiss hojayegi or after rebalancing it focuses on last commit jo ki 3 sec phle ka hoga because prcessing complete nhi hui iski  vjaah se duplicate messages processing hongi.

# as a developer , hmko do aspects pr kam krna he :
    # first one is try elimating missing of messages.
    # second one is try reducing processing of duplicate messages.

# phla ques : ab isko reduce kren kese : think of this like sliding window we kanow that certain time pr commit call hora he , and we know that if we want our commit to be independent of rebalancing we have to reduce the time window size as possible to reduce effect but we cannot eliminate it completely.
# second ques : mene upar esa kyon likha "try elimating missing of messages" ? missing message ki to bat hi ni hui thi right ?? 
 # scenario 2 : suppose consumer data process krne me time lera he due to multiple operations , but offset keep going and there is a case where my currect offset if far behind the latest commited offset which result in missing event messages if next event et processed .


# thank you this is the complete concept 
