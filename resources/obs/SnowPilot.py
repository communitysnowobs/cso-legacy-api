import sys
import time

import pandas as pd
import requests

from flask_restful import fields, marshal, reqparse, Resource, inputs
from common.utils import unsafe, cache, empty_cso_dataframe
from common.elevation import with_elevation

BASE_URL = 'http://snowpilot.org/snowpilot-query-feed.xml'
HEADER = {
    'Content-Disposition': 'attachment; filename="query-results.xml"',
    'Content-Type': 'application/xml'
}
ONE_MONTH = 2592000000

class SnowPilot():

    def __init__(self):
        self.key = "SnowPilot"
        self.state = { 'min_page': 1, 'max_page': 1 }

    def get_new_data(self):
        print("Getting new data!")
        if ('max_page' in self.state):
            pg = self.state['max_page']
            for block in self.__get_data(pg):
                yield block
        print("Finished getting new data!")

    def get_all_data(self):
        print("Getting all data!")
        if ('min_page' in self.state):
            pg = self.state['min_page']
            for block in self.__get_data(pg):
                yield block

        print("Finished getting all data!")

    def __get_data(self, pg):
        while pg < 48:
            block = self.__fetch_raw_data(pg)
            yield block
            pg += 1
            time.sleep(60)
        self.state['max_page'] = pg

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

    def __fetch_raw_data(self, page):

        args = {
            'page' : page,
            'LOC_NAME': '',
            'OBS_DATE_MIN': '',
            'OBS_DATE_MAX': '',
            'USERNAME': '',
            'AFFIL': '',
            'per_page': '100',
            'submit': 'Get Pits'
        }

        response = requests.get(BASE_URL, params=args, headers=HEADER)

        print(response.text)
        return empty_cso_dataframe()

        data = response.json()

        if 'results' not in data or len(data['results']) == 0:
            return empty_cso_dataframe()
        else:
            results = data['results']
            all_results = [ self.__parse_data(result) for result in results ]
            valid_results = [result for result in all_results if result is not None]
            valid_results = with_elevation(valid_results)
            return pd.DataFrame.from_records(valid_results)
