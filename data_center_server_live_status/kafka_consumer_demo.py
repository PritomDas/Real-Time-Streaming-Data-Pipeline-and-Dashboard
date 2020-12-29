from kafka import KafkaConsumer
from json import loads

KAFKA_CONSUMER_GROUP_NAME_CONS = "test_consumer_group"
KAFKA_TOPIC_NAME_CONS = "server-live-status"
KAFKA_BOOTSTRAP_SERVERS_CONS = '192.168.99.100:9092'

if __name__ == "__main__":

    print("Kafka Consumer Application Started ... ")

    try:
        # auto_offset_reset='latest'
        # auto_offset_reset='earliest'
        consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME_CONS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
        value_deserializer=lambda x: loads(x.decode('utf-8')))

        for message in consumer:
            #print(dir(message))
            #print(type(message))
            print("Key: ", message.key)
            message = message.value
            print("Message received: ", message)
    except Exception as ex:
        print("Failed to read kafka message.")
        print(ex)

    print("Kafka Consumer Application Completed. ")
