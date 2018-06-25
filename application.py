from flask import Flask
from flask_restful import Api
from resources.cso import CSO
from resources.snodas import SNODAS
from resources.obs.Obs import Obs
from resources.obs.Obs_Database import Obs_Database

from dotenv import load_dotenv
load_dotenv()

application = Flask(__name__)
api = Api(application)

db = Obs_Database()

api.add_resource(CSO, '/cso')
api.add_resource(SNODAS, '/snodas')
api.add_resource(Obs, '/obs', resource_class_args = [db])

if __name__ == '__main__':
    application.run(debug=True)
