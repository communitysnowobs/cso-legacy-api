import sys
import time

import pandas as pd
import geopandas as gpd
import requests
import os

from lxml import etree
import time

from flask_restful import fields, marshal, reqparse, Resource, inputs
from common.utils import empty_cso_dataframe, timestamp_to_date
from common.elevation import with_elevation
from common.decorators import unsafe, cache

LOGIN_URL = 'https://snowpilot.org/user/login'
LOGIN_HEADERS = { 'User-Agent': 'script login' }
BASE_URL = 'https://snowpilot.org/snowpilot-query-feed.xml'
HEADER = {
    'Content-Disposition': 'attachment; filename="query-results.xml"',
    'Content-Type': 'application/xml'
}
ONE_MONTH = 2592000000

class SnowPilot():

    def __init__(self):
        self.key = "SnowPilot"
        self.state = { 'min_timestamp': 1476705600000, 'max_timestamp': 1476705600000 }
        self.cookies = self.get_cookies()

    def get_cookies(self):
        post_data = {
            'name' : os.getenv('SNOWPILOT_USERNAME'),
            'pass' : os.getenv('SNOWPILOT_PASSWORD'),
            'form_id' : 'user_login',
            'op' : 'Log in'
        }
        print(os.getenv('GOOGLE_API_KEY'))
        print(post_data)
        r = requests.post(LOGIN_URL, data = post_data, headers=LOGIN_HEADERS)
        return r.history[0].cookies

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

        dict = {
            **record.attrib,
            **record[0].attrib
        }
        cso_format = {
            'author_name' : fields.String(attribute=lambda x: " ".join([x['first'], x['last']])),
            'id' : fields.String(attribute = 'nid'),
            'timestamp' : fields.Integer(attribute = lambda x: int(x['timestamp'])),
            'lat' : fields.Float(attribute = lambda x: float(x['lat'])),
            'long' : fields.Float(attribute = lambda x: float(x['longitude'])),
            'snow_depth' : fields.Float(attribute = lambda x: float(x['heightOfSnowpack'])),
            'source' : fields.String(default='SnowPilot')
        }

        return marshal(dict, cso_format)

    def __fetch_raw_data(self, min_timestamp, max_timestamp):

        date_min = timestamp_to_date(min_timestamp).strftime('%Y-%m-%d')
        date_max = timestamp_to_date(max_timestamp).strftime('%Y-%m-%d')

        args = {
            'LOC_NAME': '',
            'OBS_DATE_MIN': date_min,
            'OBS_DATE_MAX': date_max,
            'USERNAME': '',
            'AFFIL': '',
            'per_page': '1000',
            'submit': 'Get Pits'
        }

        response = requests.get(BASE_URL, params=args, headers=HEADER, cookies=self.cookies)
        if response.status_code == 200:
            try:
                xml = etree.XML(response.text.replace('<?xml version="1.0" encoding="UTF-8"?>\n', ''))
                results = xml.getchildren()
                all_results = [ self.__parse_data(result) for result in results]
                valid_results = [result for result in all_results if result is not None]
                valid_results = with_elevation(valid_results)
                if len(valid_results) > 0:
                    df = pd.DataFrame.from_records(valid_results)
                    return df
            except Exception as e:
                print(e)
                #print(response.text)

        return empty_cso_dataframe()

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
