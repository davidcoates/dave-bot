from typing import *
from datetime import datetime, timezone

import logging
import influxdb
import os

INFLUX_USERNAME = os.getenv('INFLUX_USERNAME')
INFLUX_PASSWORD = os.getenv('INFLUX_PASSWORD')

logging.info(f"influx credentials({INFLUX_USERNAME}:{INFLUX_PASSWORD})")

INFLUX_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

def to_influx_time(time: datetime):
    return time.strftime(INFLUX_TIME_FORMAT)

def from_influx_time(time: str):
    return datetime.strptime(time, INFLUX_TIME_FORMAT)

class InfluxDBClient:

    def __init__(self):
        self._impl = influxdb.InfluxDBClient(host="undergroundantics.au", port=8086, username=INFLUX_USERNAME, password=INFLUX_PASSWORD)
        self._impl.switch_database('davebot')

    def write(self, measurement: str, tags, fields, time: Optional[datetime] = None):
        if time is None:
            time = datetime.now(timezone.utc)
        point = {
            'measurement': measurement,
            'time': to_influx_time(time),
            'tags': tags,
            'fields': fields
        }
        ok = self._impl.write_points([point])
        assert ok

    def query(self, query):
        return self._impl.query(query)
