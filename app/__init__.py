import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from app.helper import setup_loggin
app = Flask(__name__)
logger = setup_loggin()

config_name = os.getenv('env', 'Production')

app.config.from_object(f'config.{config_name}')
db = SQLAlchemy(app)

from app.routes import *
from app.models import *
