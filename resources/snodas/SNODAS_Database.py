import pandas as pd
import geopandas as gpd
import os
import pickle
import threading
from threading import Lock
import time
import schedule
import json
import xarray as xr

from datetime import datetime, timedelta
from common.utils import cache, empty_cso_dataframe, error_message, data_message, threaded
import common.utils as ut
import resources.snodas.SNODAS_Retrieve as SNODAS_Retrieve

class SNODAS_Database():

    def __init__(self, store_dir = ".store/snodas"):

        if not os.path.exists(store_dir):
            os.makedirs(store_dir)

        self.store_dir = store_dir
        self.state_path = os.path.join(self.store_dir, "state")
        self.nc_path_1 = os.path.join(self.store_dir, "db_1.nc")
        self.nc_path_2 = os.path.join(self.store_dir, "db_2.nc")

        self.state = self.load()

        self.ds = None
        self.nc_lock_1 = Lock()
        self.nc_lock_2 = Lock()
        self.ds_lock = Lock()

        self.get_next_data()

    def load(self):
        return self.load_state()

    def load_state(self):
        if os.path.exists(self.state_path):
            with open(self.state_path, 'rb') as state_file:
                return pickle.load(state_file)
        else:
            return {
                'min_date' : datetime(2018,1,1),
                'max_date_1' : None,
                'max_date_2' : None
            }

    def save(self):
        with open(self.state_path, 'wb') as state_file:
            pickle.dump(self.state, state_file)

    @threaded
    def get_next_data(self):
        if 'max_date_1' not in self.state or self.state['max_date_1'] is None:
            self.create_db(self.state['min_date'])
        else:
            self.append_db(self.state['max_date_1'] + timedelta(days=1))
        self.save()

    def get_snodas(self, date):
        path = os.path.join(self.store_dir, date.strftime('SNODAS_%Y%m%d.nc'))
        if not os.path.exists(path):
            snodas_ds = SNODAS_Retrieve.snodas_ds(date)
            ut.save_netcdf(snodas_ds, path)

        return path

    def create_db(self, date):

        path = self.get_snodas(date)
        ncap_format = "ncap2 -O -s \"time=%d\" -s \"time@units=\\\"days since %s\\\"\" %s %s"
        ncap_str = ncap_format % (0, date, path, path)
        os.system(ncap_str)

        ncecat_format = "ncecat -O --no_tmp_fl --open_ram -u time %s %s"
        ncecat_str_1 = ncecat_format % (path, os.path.join(self.store_dir, "db_1.nc"))
        ncecat_str_2 = ncecat_format % (path, os.path.join(self.store_dir, "db_2.nc"))

        self.nc_lock_1.acquire()
        os.system(ncecat_str_1)
        self.nc_lock_1.release()

        self.nc_lock_2.acquire()
        os.system(ncecat_str_2)
        self.nc_lock_2.release()

        os.system("rm %s" % path)

    def append_db(self, date):

        path = self.get_snodas(date)
        ncap_format = "ncap2 -O -s \"time=%d\" -s \"time@units=\\\"days since %s\\\"\" %s %s"
        ncap_str = ncap_format % ((date-self.state['min_date']).days, self.state['min_date'], path, path)
        os.system(ncap_str)

        ncecat_format = "ncecat -O --open_ram -u time %s %s"
        ncecat_str = ncecat_format % (path, path)
        os.system(ncecat_str)

        ncrcat_format = "ncrcat --rec_apn --no_tmp_fl %s %s"
        ncrcat_str_1 = ncrcat_format % (output_path, os.path.join(self.dir, "db_1.nc"))
        ncrcat_str_2 = ncrcat_format % (output_path, os.path.join(self.dir, "db_1.nc"))

        os.system(ncrcat_str_1)
        os.system(ncrcat_str_2)
        os.system("rm %s" % path)


    def run_worker(self):
        #schedule.every(1).hour.do(SNODAS_Database.get_new_data, self)
        #schedule.every(1).days.at("00:00").do(SNODAS_Database.get_all_data, self)
        while True:
            schedule.run_pending()
            time.sleep(60)

    @cache(ttl=60, max_size = 128)
    def query(self, lat, long):
        ds = xr.open_dataset(self.nc_path_1)

        series = ds.Band1.sel(lat=lat, lon=long, method='nearest') / 10
        timestamps = [int(x.astype('datetime64[s]').astype('int')) for x in series.coords['time'].values]
        depths = [float(val) for val in series.values]

        res = [{'snow_depth' : depth, 'timestamp' : timestamp} for depth, timestamp in zip(depths, timestamps)]
        return {'results' : res }
