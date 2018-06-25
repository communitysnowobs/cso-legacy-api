from typing import Optional, Dict, List, Any

import requests

from flask_restful import fields, marshal, reqparse, Resource, inputs
from datetime import datetime
from common.utils import unsafe, cache
from common.elevation import with_elevation

BASE_URL = 'https://api.mountainhub.com/timeline'
HEADER = { 'Accept-version': '1' }

parser = reqparse.RequestParser(bundle_errors=True)
parser.add_argument('limit', type=int, default=500)
parser.add_argument('start', dest='since', type=int, default=1484862400000)
parser.add_argument('end', dest='before', type=int, default=1534398400000)
parser.add_argument('with_elevation', type=inputs.boolean, default=True)

args_format = {
    'publisher' : fields.String(default='all'),
    'obs_type' : fields.String(default='snow_conditions'),
    'limit' : fields.Integer,
    'since' : fields.Integer,
    'before' : fields.Integer,
    'with_elevation' : fields.Boolean
}

cso_format = {
    'author_name' : fields.String(attribute='actor.full_name') or fields.String(attribute='actor.fullName'),
    'id' : fields.String(attribute=lambda x: x['observation']['_id']),
    'timestamp' : fields.Integer(attribute=lambda x: x['observation']['reported_at']),
    'lat' : fields.Float(attribute=lambda x: x['observation']['location'][1]),
    'long' : fields.Float(attribute=lambda x: x['observation']['location'][0]),
    'type' : fields.String(attribute=lambda x: x['observation']['type']),
    'snow_depth' : fields.Float(attribute=lambda x: x['observation']['details'][0]['snowpack_depth'])
}

@unsafe
def parse_cso(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return marshal(record, cso_format)

@cache(ttl=3600, max_size=256)
def get_cso(**kwargs) -> Dict[str, Any]:
    response = requests.get(BASE_URL, params=kwargs, headers=HEADER)
    data = response.json()
    if 'results' not in data:
        return data
    else:
        results = data['results']
        print("LEN: %d" % len(results))
        all_results = [ parse_cso(result) for result in results ]
        valid_results = [result for result in all_results if result is not None]
        if kwargs['with_elevation']:
            valid_results = with_elevation(valid_results)

        return {'results' : valid_results }


class CSO(Resource):
    def get(self) -> Dict[str, List]:
        args = marshal(parser.parse_args(), args_format)
        return get_cso(**dict(args))
