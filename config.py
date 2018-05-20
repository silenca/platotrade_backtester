import os
basedir = os.path.abspath(os.path.dirname(__file__))

class Config(object):

    DEBUG = False
    SERVER_NAME = 'localhost:8000'

class Production(Config):

    pass


class Local(Config):

    ENV ='local'
    SERVER_NAME = 'localhost:5000'
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:1@localhost/platotrading'

