#import functions and variables
from extractFunctions import *

# get data and send to SQL - specify three airline stocks foe example.

# IAG.L = International Consolidated Airlines Group, S.A.
# 0293.HK = Cathay Pacific Airways Ltd
# AF.PA = Air France-KLM SA

# send individual tables to sql for each stock
# individualTables(['IAG.L', '0293.HK', 'AF.PA'])

# combine tables and send to sql
stock_list = ['IAG.L', '0293.HK', 'AF.PA']
combined_tables(stock_list)
# exchange_rate_table(stock_list, period='1y', interval='1d')
