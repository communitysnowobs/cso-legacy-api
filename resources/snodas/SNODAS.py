import json
import time
import math

from flask_restful import fields, marshal, reqparse, Resource, inputs
from datetime import datetime
from common.utils import most_recent_hour
from common.elevation import with_elevation

parser = reqparse.RequestParser(bundle_errors=True)
parser.add_argument('lat', type=float, required=True)
parser.add_argument('long', type=float, required=True)

args_format = {
    'lat' : fields.Float(),
    'long' : fields.Float()
}

args_format = {
    'lat' : fields.Float(),
    'long' : fields.Float()
}

class SNODAS(Resource):

    def __init__(self, db):
        self.db = db

    def get(self):
        args = marshal(parser.parse_args(), args_format)
        return self.db.query(**dict(args))
