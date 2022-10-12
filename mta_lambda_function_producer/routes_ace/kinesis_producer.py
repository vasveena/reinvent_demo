import os
import time
from google.protobuf.json_format import MessageToJson
from google.transit import gtfs_realtime_pb2
import requests
from json import dumps
import underground
from underground import metadata, feed
import boto3

class MTARealTime(object):

    def __init__(self):
        self.url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace'
        self.api_key = os.environ.get('MTA_API_KEY')
        self.kinesis_tripupdate_stream = 'trip-update-stream'
        self.kinesis_vehicle_stream = 'trip-status-stream'
        self.kinesis_client = boto3.client('kinesis', region_name='us-east-1')

    def produce_trip_updates(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(self.url, headers={"x-api-key": self.api_key})
        feed.ParseFromString(response.content)
        #print(feed)
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                feed_tu_json = MessageToJson(entity.trip_update)
                update_tu_json = str(feed_tu_json.replace('\n', '').replace(' ','').strip()).encode('utf-8')
                self.kinesis_client.put_record(StreamName=self.kinesis_tripupdate_stream,Data=update_tu_json,PartitionKey="trip.tripId")
            if entity.HasField('vehicle'):
                feed_vh_json = MessageToJson(entity.vehicle)
                update_vh_json = str(feed_vh_json.replace('\n', '').replace(' ','').strip()).encode('utf-8')
                self.kinesis_client.put_record(StreamName=self.kinesis_vehicle_stream,Data=update_vh_json,PartitionKey="trip.tripId")

    def run(self):
        while True:
            self.produce_trip_updates()
            time.sleep(10)

if __name__ == '__main__':
    MTARealTime().run()
