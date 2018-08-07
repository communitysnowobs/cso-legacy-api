import json
import time
import math

from flask_restful import fields, marshal, reqparse, Resource, inputs
from datetime import datetime
from common.utils import most_recent_hour
from common.elevation import with_elevation

parser = reqparse.RequestParser(bundle_errors=True)
parser.add_argument('limit', type=int, default=100)
parser.add_argument('start', type=int, default=1457458000000)
parser.add_argument('end', type=int, default = 0)
parser.add_argument('page', type=int, default=1)
parser.add_argument('region', type=str, default = '')
parser.add_argument('source', type=str, default = '')

args_format = {
    'limit' : fields.Integer,
    'start' : fields.Integer,
    'end' : fields.Integer(default=int(time.time() * 1000)),
    'page' : fields.Integer,
    'region' : fields.String,
    'source' : fields.String
}

class Obs(Resource):

    def __init__(self, db):
        self.db = db

    def get(self):
        args = marshal(parser.parse_args(), args_format)
        args['end'] = args['end'] or most_recent_hour()
        return self.db.query(**dict(args))
