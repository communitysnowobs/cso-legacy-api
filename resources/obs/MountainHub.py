import sys
import time

import pandas as pd
import geopandas as gpd
import requests

from flask_restful import fields, marshal, reqparse, Resource, inputs
from common.utils import empty_cso_dataframe
from common.elevation import with_elevation
from common.decorators import unsafe, cache

BASE_URL = 'https://api.mountainhub.com/timeline'
HEADER = { 'Accept-version': '1' }
ONE_MONTH = 2592000000

class MountainHub():

    def __init__(self):
        self.key = "MountainHub"
        self.state = { 'min_timestamp': 1427458000000, 'max_timestamp': 1427458000000 }

    def get_new_data(self):
        if ('max_timestamp' in self.state):
            ts = self.state['max_timestamp']
            for block in self.__get_data(ts):
                yield block
            self.state['max_timestamp'] = int(time.time() * 1000)

    def get_all_data(self):
        if ('min_timestamp' in self.state):
            ts = self.state['min_timestamp']
            for block in self.__get_data(ts):
                yield block

    def __get_data(self, timestamp):
        while timestamp < int(time.time() * 1000):
            block = self.__fetch_raw_data(timestamp, min(timestamp + ONE_MONTH, int(time.time() * 1000)))
            yield block
            timestamp += ONE_MONTH

    @unsafe
    def __parse_data(self, record):

        cso_format = {
            'author_name' : fields.String(attribute='actor.full_name') or fields.String(attribute='actor.fullName'),
            'id' : fields.String(attribute=lambda x: x['observation']['_id']),
            'timestamp' : fields.Integer(attribute=lambda x: x['observation']['reported_at']),
            'lat' : fields.Float(attribute=lambda x: x['observation']['location'][1]),
            'long' : fields.Float(attribute=lambda x: x['observation']['location'][0]),
            'type' : fields.String(attribute=lambda x: x['observation']['type']),
            'snow_depth' : fields.Float(attribute=lambda x: x['observation']['details'][0]['snowpack_depth'])
        }

        return marshal(record, cso_format)

    def __fetch_raw_data(self, min_timestamp, max_timestamp):

        args = {
            'publisher' : 'all',
            'obs_type' : 'snow_conditions',
            'limit' : 10000,
            'since' : min_timestamp,
            'before' : max_timestamp,
        }

        response = requests.get(BASE_URL, params=args, headers=HEADER)
        data = response.json()

        if 'results' not in data or len(data['results']) == 0:
            return empty_cso_dataframe()
        else:
            results = data['results']
            all_results = [ self.__parse_data(result) for result in results ]
            valid_results = [result for result in all_results if result is not None]
            valid_results = with_elevation(valid_results)
            if len(valid_results) == 0:
                return empty_cso_dataframe()

            df = pd.DataFrame.from_records(valid_results)
            return df
