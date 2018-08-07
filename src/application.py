import os

from flask import Flask
from flask_restful import Api
from resources.snodas.SNODAS import SNODAS
from resources.obs.Obs import Obs
from resources.obs.Obs_Database import Obs_Database
from resources.snodas.SNODAS_Database import SNODAS_Database

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

application = Flask(__name__)
application.config['RESTFUL_JSON'] = {'indent': 4}

api = Api(application)

obs_db = Obs_Database()
api.add_resource(Obs, '/obs', resource_class_args = [obs_db])

if __name__ == '__main__':
    application.debug = False
    application.run(host='0.0.0.0', port = os.getenv('PORT', 5000))
