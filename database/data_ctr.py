import os
import json

from fredapi import Fred

import securities.equities as equities
import securities.fx as fx
import db.db_service as db_service


class DataController:

    def __init__(self, config_file_path='/Users/yonas/Documents/Git/quant/database/config.json'):
        with open(config_file_path,'r') as f:
            config = json.load(f)
            os.environ['FRED_KEY'] = config['fred_key']
            os.environ['MONGO_DB'] = config['mongo_db']
            os.environ['MONGO_USER'] = config['mongo_user']
            os.environ['MONGO_PW'] = config['mongo_pw']
            os.environ['MONGO_CLUSTER'] = config['mongo_cluster']

    
        
        self.fred_client = Fred(api_key=os.getenv('FRED_KEY'))
        self.data_clients = {'fred_client': self.fred_client}
        self.db_service = db_service.DBService()
        self.equities = equities.Equities(
                        data_clients=self.data_clients,
                        db_service=self.db_service
        )
        self.fx = fx.FX(
            data_clients=self.data_clients,
            db_service=self.db_service
        )

    def get_fx_service(self):
        return self.fx
    
    def get_equity_service(self):
        return self.equities

