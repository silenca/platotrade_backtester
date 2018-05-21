import os
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):

    DEBUG = False


class Production(Config):

    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://platotrading:12platotrading12@127.0.0.1/platotrading'


class Local(Config):

    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:1@localhost/platotrading'

