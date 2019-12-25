from __future__ import division
import os
from flask import Flask
from flask import render_template
from flask import request
from flask import redirect

from pykafka import KafkaClient
import pandas as pd
from pykafka import KafkaClient, SslConfig
from pykafka import topic
from pykafka.common import OffsetType
from kafka.admin import KafkaAdminClient, NewTopic
import struct
import time
import json
import numpy as np


import math
from itertools import islice
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyser = SentimentIntensityAnalyzer()


app = Flask(__name__)

def create_topic(name):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')
    topic_list = []
    topic_list.append(NewTopic(name=str(name), num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

client = KafkaClient(hosts="127.0.0.1:9092")

def create():
    try:
        print ("\n\n Creating Topic \n\n")
        create_topic("sentiment")
        topic = client.topics['sentiment']
        with topic.get_sync_producer() as producer:
            for i in range(30):
                value = 'SecRet5543Xf'
                producer.produce(value.encode())
    except Exception as e:
        print ("\n\n Sentiment Already exists \n\n")
        print (e)


def sentscore(sentence):
    sentence = str(sentence)
    if sentence == 'SecRet5543Xf':
        return {'compound':float(0)}
    score = analyser.polarity_scores(sentence)
    return score

@app.route('/')
def home():
    topic = client.topics['sentiment']
    consumer = topic.get_simple_consumer(auto_offset_reset=OffsetType.LATEST,reset_offset_on_start=True)
    LAST_N_MESSAGES = 25
    MAX_PARTITION_REWIND = int(math.ceil(LAST_N_MESSAGES / len(consumer._partitions)))
    offsets = [(p, op.last_offset_consumed - MAX_PARTITION_REWIND)for p, op in consumer._partitions.items()]
    offsets = [(p, (o if o > -1 else -2)) for p, o in offsets]
    consumer.reset_offsets(offsets)
    dat = []
    offset = []
    for message in islice(consumer, LAST_N_MESSAGES):
        incoming_data = message.value.decode()
        value = sentscore(incoming_data)['compound']
        dat.append(value)
        offset.append(message.offset)
    return render_template("home.html", sentdata = dat[10:], ofs = offset[10:], hist = dat)


@app.route('/admin', methods=["GET","POST"])
def admin():
    if request.form:
        incoming_data = request.form.get("opinion")
        print (request.form.get("opinion"))
        topic = client.topics['sentiment']
        with topic.get_sync_producer() as producer:
            value = incoming_data
            producer.produce(value.encode())
    return render_template("admin.html")

if __name__ == "__main__":
    create()
    app.run(debug=True, threaded=True)
