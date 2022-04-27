#import libs
from extractFunctions import companies_returned

#get data and send to SQL - specify three airline stocks

# IAG.L = International Consolidated Airlines Group, S.A.
# 0293.HK = Cathay Pacific Airways Ltd
# AF.PA = Air France-KLM SA
companies_returned(['IAG.L', '0293.HK', 'AF.PA'])