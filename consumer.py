#Dependencies
from json import loads
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
import os
import re

# Kafka Variables
Consumer_Subscription = "ipPush4"
Attack_Producer = "AttackPush"

# Consumer - Subscribing to Raw Feed from Kafka
consumer = KafkaConsumer(
    "ipPush4",
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

print('consumer created')

# Producer - Sending Attack IP Addresses to Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer=lambda x: dumps(x).encode('utf-8'))
print('producer created')
# Temp Lists
ConsumedMessages = []
Message_Values = []
ip_Addresses = []
AttackIP_1 = []
Odometer = 0
TempDict1 = dict()
print('lists created')
# Process Subscription

for message in consumer:
    print(message)

# for message in consumer:
#     # Returns the kafka consumer object, which has meta data in addition to the list of string values
#     ConsumedMessages.append(message)
#     # Returns the list of string values pushed by the producer
#     Message_Values.append(message.value)
#     # This is the ip address of the message return
#     ip_address_return = message.value[0]
#     ip_Addresses.append(ip_address_return)

    


#     if ip_address_return not in TempDict1:
#         TempDict1[ip_address_return] = 1
#     else:
#         TempDict1[ip_address_return] = TempDict1[ip_address_return] + 1
#         if TempDict1[ip_address_return] > 4 and ip_address_return not in AttackIP_1:
           
#             # Append Attack IP to AttackIP_1 List
#             AttackIP_1.append(ip_address_return)
#             # Send Attack IP to Kafka topic AttackIP
#             producer.send(Attack_Producer, value=ip_address_return)

#             print(f'Identified as Attack IP: {ip_address_return}')
#             recentAttack = ip_address_return
#     print(f'List item number processed: {Odometer}')
#     recentAttack = ip_address_return
# # Identify total number of attacks
# Total_attacks = 0
# for item in AttackIP_1:
#     attacks = TempDict1[item]
#     Total_attacks = Total_attacks + attacks

#  Create Summary Report
print(15*"-")
print("Processing Summary")
print("")
# print(f'Total items processed:                     {len(ConsumedMessages)}')
# print(f'Total unique IP Addresses in TempDict1:    {len(TempDict1)}')
# print(f'Total Attack IP Addresses identified:      {len(AttackIP_1)}')
# print(f'Total number of individual attacks:        {Total_attacks}')
# print(f'Example entry of an attack: IP address:    {recentAttack} / number of hits:{TempDict1[recentAttack]}')

