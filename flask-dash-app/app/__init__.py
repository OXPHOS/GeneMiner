# Start flask
from flask import Flask
app = Flask(__name__)

# Connect to Psql
from src import __credential__
import psycopg2

conn = psycopg2.connect(host=__credential__.host_psql, dbname=__credential__.dbname_psql,
                        user=__credential__.user_psql, password=__credential__.password_psql)

# Run
from app import dashapp
