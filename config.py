import os
import json
import configparser

from app.services.decorators.signleton import SingletonDecorator

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):

    DEBUG = False


class Production(Config):

    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:@127.0.0.1/platotrading'
    SQLALCHEMY_TRACK_MODIFICATIONS = False

class Local(Config):

    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:1@localhost/platotrading'

@SingletonDecorator
class FileConfig():

    def __init__(self):
        self.config = configparser.ConfigParser()
        with open(os.path.join(basedir, 'config.json'), 'r') as f:
            self.config = json.load(f)

    def get(self, key, default=None, parser=None):
        section, option = key.split('.')
        if section not in self.config:
            return default
        if option not in self.config[section]:
            return default
        if parser is None:
            parser = lambda x:x
        if parser is bool:
            parser = lambda x:bool(int(x))
        return parser(self.config[section][option])