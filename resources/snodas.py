from typing import Optional, Dict, List, Any

import requests
import xarray as xr
from flask_restful import fields, marshal, reqparse, Resource, inputs
from datetime import datetime

from common.utils import cache

parser = reqparse.RequestParser(bundle_errors=True)
parser.add_argument('lat', type=float, required=True)
parser.add_argument('long', type=float, required=True)

args_format = {
    'lat' : fields.Float(),
    'long' : fields.Float()
}

ds = xr.open_dataset('data/SNODAS/SNODAS_2017_2018.nc')

@cache(ttl=3600, max_size=256)
def get_snodas(**kwargs) -> Dict[str, List]:
    series = ds.Band1.sel(lat=kwargs['lat'], lon=kwargs['long'], method='nearest') / 10
    timestamps = [int(x.astype('datetime64[s]').astype('int')) for x in series.coords['time'].values]
    depths = [float(val) for val in series.values]

    res = [{'snow_depth' : depth, 'timestamp' : timestamp} for depth, timestamp in zip(depths, timestamps)]
    return {'results' : res }

class SNODAS(Resource):
    def get(self) -> Dict[str, List]:
        args = marshal(parser.parse_args(), args_format)
        return get_snodas(**dict(args))
