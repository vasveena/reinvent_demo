import os
import time
from google.protobuf.json_format import MessageToJson
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2
import requests
from json import dumps
import underground
from underground import metadata, feed
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class MTARealTime(object):

    def __init__(self):
        self.url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs'
        self.api_key = os.environ.get('MTA_API_KEY')
        self.kafka_tripupdate_topic = 'trip_update_topic'
        self.kafka_vehicle_topic = 'trip_status_topic'
        self.kafka_producer = KafkaProducer(security_protocol="SSL", bootstrap_servers=['b-3.democluster2.exng22.c17.kafka.us-east-1.amazonaws.com:9094','b-2.democluster2.exng22.c17.kafka.us-east-1.amazonaws.com:9094','b-1.democluster2.exng22.c17.kafka.us-east-1.amazonaws.com:9094'])

    def produce_trip_updates(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(self.url, headers={"x-api-key": self.api_key})
        feed.ParseFromString(response.content)
        #print(feed)
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                feed_tu_json = MessageToJson(entity.trip_update)
                update_tu_json = str(feed_tu_json.replace('\n', '').replace(' ','').strip()).encode('utf-8')
                self.kafka_producer.send(topic=self.kafka_tripupdate_topic, value=update_tu_json)
            if entity.HasField('vehicle'):
                feed_vh_json = MessageToJson(entity.vehicle)
                update_vh_json = str(feed_vh_json.replace('\n', '').replace(' ','').strip()).encode('utf-8')
                self.kafka_producer.send(topic=self.kafka_vehicle_topic, value=update_vh_json)
        self.kafka_producer.flush()

    def run(self):
        while True:
            self.produce_trip_updates()
            time.sleep(10)

def lambda_handler(event, context):
    MTARealTime().run()
    return None
