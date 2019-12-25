from pykafka import KafkaClient
import pandas as pd
from pykafka import KafkaClient, SslConfig
from pykafka import topic
from pykafka.common import OffsetType
from kafka.admin import KafkaAdminClient, NewTopic
import time
import random

client = KafkaClient(hosts="127.0.0.1:9092")



topic = client.topics['sentiment']

def main():

    with open('random_comments.txt','r') as f:
        df = f.read()

    df = df.split("\n")
    df = [i[0:-1].strip() for i in df]
    while True:
        with topic.get_sync_producer() as producer:
            time.sleep(2)
            value = random.choice(df)
            producer.produce(value.encode())


if __name__ == '__main__':
    main()
