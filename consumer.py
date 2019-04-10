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
    "ipTest15",
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=False,
     consumer_timeout_ms=60000,
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
    ConsumedMessages.append(message)
    Message_Values.append(message.value)

    IP_Address = message.value[0]
    ip_Addresses.append(IP_Address)

   
    # Add to dictionary or add value to dictionary if entry already exists
    if IP_Address not in TempDict1:
        TempDict1[IP_Address] = 1
    else:
        TempDict1[IP_Address] = TempDict1[IP_Address] + 1
        if TempDict1[IP_Address] > 4 and IP_Address not in AttackIP_1:
            AttackIP_1.append(IP_Address)
            print(f'Identified as Attack IP: {IP_Address}')
            producer.send('AttackTest15', value=IP_Address)

            recentAttack = IP_Address


            print(f'Identified as Attack IP: {recentAttack}')

# Identify total number of attacks
Total_attacks = 0
for item in AttackIP_1:
    attacks = TempDict1[item]
    Total_attacks = Total_attacks + attacks

#  Create Summary Report
print(15*"-")
print("Processing Summary")
print("")
print(f'Total messages processed:        {len(ConsumedMessages)}')
print(f'Total Message Values Collected:  {len(Message_Values)}')
print(f'Total IP Addresses Collected:    {len(ip_Addresses)}')
print(f'Total entries in Dictionary:     {len(TempDict1)}')
print(f'Total attack IP_Addresses:       {len(AttackIP_1)}')
print(f'Total number of attacks:         {Total_attacks}')
