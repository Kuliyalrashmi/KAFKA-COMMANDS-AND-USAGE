from kafka import KafkaProducer
from kafka.errors import KafkaError
from fastavro.schema import load_schema
from fastavro import writer
import io

schema = load_schema(r'C:/Users/ujjwa/Desktop/Kafka_series/learning_kafka/Kafka_Consumer/Serializer_Kafka_with_Avro/user_personal_info.avsc')

class UserInfo:
    class UserSubAccount:
        def __init__(self, AccountId, AccountType):
            """
            Initialize the UserSubAccount class.

            :param AccountId: Account ID (int)
            :param AccountType: Account type (string)
            """
            self.AccountId = AccountId
            self.AccountType = AccountType

        def __repr__(self):
            return f"UserSubAccount(AccountId={self.AccountId}, AccountType='{self.AccountType}')"

        def to_dict(self):
            return {"AccountId": self.AccountId, "AccountType": self.AccountType}

    def __init__(self, first_name, family_name, adhar_number, pan_number, current_job, contact_number, address, Accounts=None):
        self.first_name = first_name
        self.family_name = family_name
        self.adhar_number = adhar_number
        self.pan_number = pan_number
        self.current_job = current_job
        self.contact_number = contact_number
        self.address = address
        self.Accounts = Accounts or []

    def to_dict(self):
        return {
            "first_name": self.first_name,
            "family_name": self.family_name,
            "adhar_number": self.adhar_number,
            "pan_number": self.pan_number,
            "current_job": self.current_job,
            "contact_number": self.contact_number,
            "address": self.address,
            "Accounts": [account.to_dict() for account in self.Accounts] if self.Accounts else None
        }

    def __repr__(self):
        return (f"UserInfo(first_name='{self.first_name}', family_name='{self.family_name}', adhar_number={self.adhar_number}, "
                f"pan_number='{self.pan_number}', current_job='{self.current_job}', contact_number='{self.contact_number}', "
                f"address='{self.address}', Accounts={self.Accounts})")



def avro_serializer(data):
    bytes_writer = io.BytesIO()
    writer(bytes_writer , schema  , [data])
    return bytes_writer.getvalue()


def success(metadata):
    print(f"Data Succefully written to topic : {metadata.topic} | Offset : {metadata.offset} | Partition : {metadata.partition} ")

def error(excp):
    print(f"Error : {excp}")

def main():
    producer = KafkaProducer(
        bootstrap_servers = ["localhost:9092"],
        retries = 3,
        max_block_ms = 5000,
        value_serializer = lambda value : avro_serializer(value),
        key_serializer = lambda key : key.encode('utf-8'),
        batch_size = 32000,
    )

    topic_name = "user_personal_records"

    sub_accounts = [
        UserInfo.UserSubAccount(AccountId=101, AccountType="Savings"),
        UserInfo.UserSubAccount(AccountId=102, AccountType="Current")
    ]

    user_info = UserInfo(
        first_name="John",
        family_name="Doe",
        adhar_number=123456789012,
        pan_number="ABCDE1234F",
        current_job="Software Engineer",
        contact_number="9876543210",
        address="123 Main Street, Cityville",
        Accounts=sub_accounts
    )

    producer.send(topic = topic_name ,  value = user_info.to_dict() , key = "user_123").add_callback(success).add_errback(error)

    producer.flush()

    producer.close()






if __name__ == "__main__":
    main()
