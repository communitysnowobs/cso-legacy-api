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
import subprocess

from shutil import copy2
from datetime import datetime, timedelta
from common.utils import empty_cso_dataframe, error_message, data_message
from common.decorators import cache, threaded, locked, unsafe
import common.utils as ut
import resources.snodas.SNODAS_Retrieve as SNODAS_Retrieve

# TODO - Uandle switching database in consistency check

class SNODAS_Database():

    def __init__(self, store_dir = ".store/snodas"):

        if not os.path.exists(store_dir):
            os.makedirs(store_dir)

        self.store_dir = store_dir
        self.state_path = os.path.join(self.store_dir, "state")
        self.nc_path_1 = os.path.join(self.store_dir, "db_1.nc")
        self.nc_path_2 = os.path.join(self.store_dir, "db_2.nc")

        self.state = self.load()
        if 'last_updated' in self.state and self.state['last_updated'] is not None:
            self.ds = xr.open_dataset(self.state['last_updated'])
        else:
            self.ds = None
        self.ds_lock = Lock()
        self.get_data_test()

    def load(self):
        if os.path.exists(self.state_path):
            with open(self.state_path, 'rb') as state_file:
                return pickle.load(state_file)
        else:
            return {
                'min_date' : datetime(2018,1,1),
                'max_date_1' : None,
                'max_date_2' : None,
                'last_updated' : None,
            }

    def save(self):
        with open(self.state_path, 'wb') as state_file:
            pickle.dump(self.state, state_file)

    @threaded
    @locked
    def get_data_test(self):
        for i in range(100):
            self.get_next_data()

    def get_next_data(self):
        self.make_consistent()
        if 'max_date_1' not in self.state or self.state['max_date_1'] is None:
            self.create_db(self.state['min_date'])
        else:
            self.append_db(self.state['max_date_1'] + timedelta(days=1))
        self.save()

    def create_db(self, date):

        path = SNODAS_Retrieve.save_snodas(date, self.store_dir)
        ncap_format = "ncap2 -O -s \"time=%d\" -s \"time@units=\\\"days since %s\\\"\" %s %s"
        ncap_str = ncap_format % (0, date, path, path)
        subprocess.call(ncap_str, shell=True)

        ncecat_format = "ncecat -O --no_tmp_fl --open_ram -u time %s %s"
        ncecat_str_1 = ncecat_format % (path, os.path.join(self.store_dir, "db_1.nc"))
        ncecat_str_2 = ncecat_format % (path, os.path.join(self.store_dir, "db_2.nc"))

        subprocess.call(ncecat_str_1, shell=True)
        self.state['max_date_1'] = date
        self.state['last_updated'] = self.nc_path_1
        self.save()

        self.ds_lock.acquire()
        self.ds = xr.open_dataset(self.nc_path_1)
        self.ds_lock.release()

        subprocess.call(ncecat_str_2, shell=True)
        self.state['max_date_2'] = date
        self.state['last_updated'] = self.nc_path_2
        self.save()

        subprocess.call("rm %s" % path, shell=True)

    def append_db(self, date):

        path = SNODAS_Retrieve.save_snodas(date, self.store_dir)
        ncap_format = "ncap2 -O -s \"time=%d\" -s \"time@units=\\\"days since %s\\\"\" %s %s"
        ncap_str = ncap_format % ((date-self.state['min_date']).days, self.state['min_date'], path, path)
        subprocess.call(ncap_str, shell=True)

        ncecat_format = "ncecat -O --open_ram -u time %s %s"
        ncecat_str = ncecat_format % (path, path)
        subprocess.call(ncecat_str, shell=True)

        ncrcat_format = "ncrcat --rec_apn --no_tmp_fl %s %s"

        ncrcat_str_1 = ncrcat_format % (path, os.path.join(self.store_dir, "db_1.nc"))
        ncrcat_str_2 = ncrcat_format % (path, os.path.join(self.store_dir, "db_2.nc"))
        self.ds_lock.acquire()
        self.ds = xr.open_dataset(self.nc_path_2)
        self.ds_lock.release()
        subprocess.call(ncrcat_str_1, shell=True)

        self.state['max_date_1'] = date
        self.state['last_updated'] = self.nc_path_1
        self.save()

        self.ds_lock.acquire()
        self.ds = xr.open_dataset(self.nc_path_1)
        self.ds_lock.release()
        subprocess.call(ncrcat_str_2, shell=True)

        self.state['max_date_2'] = date
        self.state['last_updated'] = self.nc_path_2
        self.save()

        subprocess.call("rm %s" % path, shell=True)

    @unsafe
    def make_consistent(self):
        # Get size of NetCDF files
        nc_1_size = os.path.getsize(self.nc_path_1)
        nc_2_size = os.path.getsize(self.nc_path_2)
        # If files are of same size, set max_date to match
        if nc_1_size == nc_2_size:
            max_date = max(self.state['max_date_1'], self.state['max_date_2'])
            self.state['max_date_1'] = max_date
            self.state['max_date_2'] = max_date
        elif 'last updated' in self.state and self.state['last_updated'] is not None:
            not_updated = self.nc_path_1 if self.state['last_updated'] == self.nc_path_2 else self.nc_path_2
            max_date = max(self.state['max_date_1'], self.state['max_date_2'])
            copy2(self.state['last_updated'], not_updated)
            self.state['max_date_1'] = max_date
            self.state['max_date_2'] = max_date
        # Copy smaller file to bigger file if inconsistent, set date accordingly
        self.save()

    @cache(ttl=10, max_size = 128)
    def query(self, lat, long):
        # Acquire lock before reading from dataset
        self.ds_lock.acquire()
        if self.ds is not None:
            series = self.ds.Band1.sel(lat=lat, lon=long, method='nearest') / 10
            timestamps = [int(x.astype('datetime64[s]').astype('int')) for x in series.coords['time'].values]
            depths = [float(val) for val in series.values]
            self.ds_lock.release()

            res = [{'snow_depth' : depth, 'timestamp' : timestamp} for depth, timestamp in zip(depths, timestamps)]
            return data_message(res)
        # Return error if dataset does not exist (no data)
        else:
            self.ds_lock.release()
            return error_message('No available data')
