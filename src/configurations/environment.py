import configparser
import base64


class Configuration:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read(["./configurations/credentials.cfg", "./configurations/redshift.cfg", "./configurations/dwh.cfg"])

        self.host = config.get("CLUSTER", "HOST")
        self.dbname = config.get("CLUSTER", "DB_NAME")
        self.user = config.get("REDSHIFT", "USER")
        self.password = base64.b64decode(config.get("REDSHIFT", "PASSWORD")).decode('utf-8')
        self.port = config.get("CLUSTER", "DB_PORT")
