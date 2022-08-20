#import libs
from extractFunctions import individualTables, combinedTables
import os
from sqlalchemy import create_engine
import logging
import sqlalchemy

logger = logging.getLogger()
logger.setLevel(logging.NOTSET)

# return error and critical logs to console
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
console_format = '%(asctime)s | %(levelname)s: %(message)s'
console.setFormatter(logging.Formatter(console_format))
logger.addHandler(console)

# create log file to capture all logging
file_handler = logging.FileHandler('dataExtract.log')
file_handler.setLevel(logging.INFO)
file_handler_format = '%(asctime)s | %(levelname)s | %(lineno)d: %(message)s'
file_handler.setFormatter(logging.Formatter(file_handler_format))
logger.addHandler(file_handler)


# get data and send to SQL - specify three airline stocks foe example.

# IAG.L = International Consolidated Airlines Group, S.A.
# 0293.HK = Cathay Pacific Airways Ltd
# AF.PA = Air France-KLM SA

# send individual tables to sql for each stock
# individualTables(['IAG.L', '0293.HK', 'AF.PA'])

# sets the environment variables as python variables
server = os.getenv('server')
username = os.getenv('sqlusername')
password = os.getenv('password')
driver = '{ODBC Driver 13 for SQL Server}'
# creates the connection string required to connect to the azure sql database
odbc_str = f'Driver={driver};Server={server},1433;Database=internalsales;Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
connect_str = f'mssql+pyodbc:///?odbc_connect={odbc_str}'
# creates the engine to connect to the database with.
# fast_executemany makes the engine insert multiple rows in each insertstatement and imporves the speed of the code drastically
engine = create_engine(connect_str,fast_executemany=True)

# combine tables and send to sql
combinedTables(['IAG.L', '0293.HK', 'AF.PA'])