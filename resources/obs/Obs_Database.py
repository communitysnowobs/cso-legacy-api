import pandas as pd
import geopandas as gpd
import os
import pickle
import threading
from threading import Lock
import time
import schedule
import json
import polyline

from shapely.geometry import Point, Polygon, shape
from resources.obs.MountainHub import MountainHub
from resources.obs.SnowPilot import SnowPilot
from common.utils import cache, empty_cso_dataframe, decoded_polygon, error_message, data_message, threaded

class Obs_Database():

    def __init__(self, store_dir = ".store/obs"):

        if not os.path.exists(store_dir):
            os.makedirs(store_dir)

        self.store_dir = store_dir
        self.state_path = os.path.join(self.store_dir, "state")
        self.df_path = os.path.join(self.store_dir, "df")
        self.df, self.state = self.load()
        self.df_lock = Lock()

        self.sources = [MountainHub()]
        # Load state into sources
        for source in self.sources:
            if source.key in self.state['sources']:
                source.state = self.state['sources'][source.key]

        self.get_new_data()

    def load(self):
        return self.load_df(), self.load_state()

    def load_df(self):
        if os.path.exists(self.df_path):
            with open(self.df_path, 'rb') as df_file:
                return pickle.load(df_file)
        else:
            return empty_cso_dataframe()


    def load_state(self):
        if os.path.exists(self.state_path):
            with open(self.state_path, 'rb') as state_file:
                return pickle.load(state_file)
        else:
            return {
                'sources' : {}
            }

    def update_df(self, new_data):
        self.df_lock.acquire()
        self.df = pd.concat([self.df,new_data], sort=False).drop_duplicates('id').reset_index(drop=True)
        self.df_lock.release()

    def save(self):
        self.save_df()
        self.save_state()

    def save_df(self):
        with open(self.df_path, 'wb') as df_file:
            self.df_lock.acquire()
            pickle.dump(self.df, df_file)
            self.df_lock.release()

    def save_state(self):
        with open(self.state_path, 'wb') as state_file:
            for source in self.sources:
                self.state['sources'][source.key] = source.state
            pickle.dump(self.state, state_file)

    def get_new_data(self):
        for source in self.sources:
            for block in source.get_new_data():
                self.update_df(block)
        self.save()

    def get_all_data(self):
        for source in self.sources:
            for block in source.get_all_data():
                self.update_df(block)
        self.save()

    @threaded
    def run_worker(self):
        schedule.every(1).hour.do(Obs_Database.get_new_data, self)
        schedule.every(1).days.at("00:00").do(Obs_Database.get_all_data, self)
        while True:
            schedule.run_pending()
            time.sleep(60)

    @cache(ttl=60, max_size = 128)
    def query(self, start, end, limit, page, region):
        # Restrict by time
        self.df_lock.acquire()
        df = self.df[(self.df.timestamp > start) & (self.df.timestamp < end)]
        self.df_lock.release()
        # Restrict by region
        if region:
            try:
                polygon = gpd.GeoSeries(decoded_polygon(region))
                locations = gpd.GeoSeries([Point(coord) for coord in zip(df['long'], df['lat'])])
                df = df[locations.intersects(polygon.ix[0])]
            except:
                return error_message('Invalid Region \'%s\'' % region)
        # Limit number of results
        df = df[((page - 1) * limit):page * limit]

        res_str = df.to_json(orient='records')
        return data_message(json.loads(res_str))
