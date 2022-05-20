#import libs
from extractFunctions import individualTables, combinedTables
import os
import sqlalchemy

#get data and send to SQL - specify three airline stocks.

# IAG.L = International Consolidated Airlines Group, S.A.
# 0293.HK = Cathay Pacific Airways Ltd
# AF.PA = Air France-KLM SA

#send individual tables to sql for each stock
#individualTables(['IAG.L', '0293.HK', 'AF.PA'])

#combine tables and send to sql
combinedTables(['IAG.L', '0293.HK', 'AF.PA'])