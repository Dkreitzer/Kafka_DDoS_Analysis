# Dependencies
from json import dumps
from kafka import KafkaProducer
import os
import re


# Raw Data - unzipped gz file
rawData = 'data/test_text3.txt'
textFile = open(rawData, "r")

Lines = []

for line in textFile:
    line.rstrip()
    line = re.sub(r'[-""]', '', line)
    entry = line.split()
    Lines.append(line)

print(15*"-")
print(f'Total items in Lines list: {len(Lines)}')

print(15*'-')
print(f'Example of item 0 in Lines list: {Lines[0]}')


# Configure Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer=lambda x: dumps(x).encode('utf-8'))

# Begin Kafka Producer
print('Producer successfully configured')

odometer = 0
for item in Lines:
    data = item
    producer.send('ipTest8', value=item)
    print(data)
    odometer = odometer + 1

print(f'Number of lines pushed to Kafka: {odometer}')

