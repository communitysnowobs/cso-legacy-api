from typing import Optional, Dict, List, Any
import os

import requests

from common.decorators import batch

BASE_ELEVATION_URL = 'https://maps.googleapis.com/maps/api/elevation/json'

@batch(size=128)
def with_elevation(data: List[Dict]) -> List[Dict]:
    params = {
        'locations': "|".join([",".join([str(x['lat']), str(x['long'])]) for x in data]),
        'key': os.getenv('GOOGLE_API_KEY')
    }
    res = requests.get(BASE_ELEVATION_URL, params=params).json()
    if 'results' not in res:
        raise ValueError(res)

    results = [{ **data, 'elevation': res['elevation'] } for data, res in zip(data, res['results'])]
    return results
