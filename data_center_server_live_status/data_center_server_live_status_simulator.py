from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
import requests
from restcountries import RestCountryApiV2 as rapi

TOPIC_NAME_CONS = "server-live-status"
BOOTSTRAP_SERVERS_CONS = '192.168.99.100:9092'
RANDOM_USER_API_URL = "https://randomuser.me/api/0.8"

if __name__ == "__main__":
    print("Data Center Server Live Status Simulator | Kafka Producer Application Started ... ")

    kafka_producer_obj = None
    kafka_producer_obj = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS_CONS,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    event_server_status_color_name_severity_level_list = ["Red|Severity 1", "Orange|Severity 2", "Green|Severity 3"]
    event_server_type_list = ["Application Servers", "Client Servers", "Collaboration Servers", "FTP Servers", "List Servers",
                        "Mail Servers", "Open Source Servers", "Proxy Servers", "Real-Time Communication Servers", "Server Platforms",
                        "Telnet Servers", "Virtual Servers", "Web Servers"]

    message = None
    i = 0
    #while True:
    while i != 10:
        try:
            #print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            response_data = requests.get(url=RANDOM_USER_API_URL)
            #print("Ramdon User Message: ")
            #print(response_data.json())
            country_code_alpha2 = response_data.json()['nationality']
            event_country_code = country_code_alpha2
            country_obj = rapi.get_country_by_country_code(alpha=country_code_alpha2)
            print(country_obj.name)
            print(country_obj.capital)

            event_country_name = country_obj.name
            event_city_name = country_obj.capital

            event_message = {}
            event_datetime = datetime.now()

            event_message["event_server_status_color_name_severity_level"] = random.choice(event_server_status_color_name_severity_level_list)
            event_message["event_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
            event_message["event_server_type"] = random.choice(event_server_type_list)
            event_message["event_country_code"] = event_country_code
            event_message["event_country_name"] = event_country_name
            event_message["event_city_name"] = event_city_name
            event_message["event_estimated_issue_resolution_time"] = round(random.uniform(1.5, 10.5))

            event_message["event_server_status_other_param_1"] = ""
            event_message["event_server_status_other_param_2"] = ""
            event_message["event_server_status_other_param_3"] = ""
            event_message["event_server_status_other_param_4"] = ""
            event_message["event_server_status_other_param_5"] = ""

            event_message["event_server_config_other_param_1"] = ""
            event_message["event_server_config_other_param_2"] = ""
            event_message["event_server_config_other_param_3"] = ""
            event_message["event_server_config_other_param_4"] = ""
            event_message["event_server_config_other_param_5"] = ""

            i = i + 1
            print("Printing message id: " + str(i))
            event_message["event_id"] = str(i)
            print("Sending message to Kafka topic: " + TOPIC_NAME_CONS)
            print("Message to be sent: ", event_message)
            kafka_producer_obj.send(TOPIC_NAME_CONS, event_message)

        except Exception as ex:
            print("Event Message Construction Failed. ")
            print(ex)

        time.sleep(1)

    print("Data Center Server Live Status Simulator | Kafka Producer Application Completed. ")
