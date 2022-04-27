#import relevant libs
import pandas as pd
from datetime import datetime
import yfinance as yf
import psycopg2
import sqlalchemy
import os
import time

# function to get stock information
def stock_sql_send(stock):
    '''
    Function to pull stock information. Currently pulls historical stock data, major shareholders,
    earnings, quarterly earnings and news.

    Arguments:
        # stock: individual stock in which user wnats to return data for.

    Return:
        # returns strings to confirm data has been sent to postgres SQL database
    '''

    #start of time function
    start = time.time()

    #path to SQL database stored as environment variable
    postgres_path = os.getenv('postgres_path')
    # dialect+driver://username:password@host:port/database
    engine = sqlalchemy.create_engine(postgres_path)

    # create ticker for the stock
    msft = yf.Ticker(stock)

    # return historical stock data and send to postgres
    stock_history = msft.history(period="max").reset_index()
    # minor cleaning
    stock_history = stock_history.rename(str.lower, axis='columns')
    # send to SQL with SQL Alchemy
    stock_history.to_sql(f'{stock}_history', engine, if_exists='replace', index=False)
    print(f'historical {stock} data sent to sql')

    # return major shareholders and send to postgres
    major_share_holders = msft.major_holders
    # minor cleaning
    major_share_holders = major_share_holders.rename(columns={0: "percent", 1: "detail"})
    # send to SQL with SQL Alchemy
    major_share_holders.to_sql(f'{stock}_major_share_holders', engine, if_exists='replace', index=False)
    print(f'major share holders {stock} data sent to sql')

    # return financials and send to postgres
    stock_financials = msft.financials.reset_index()
    # minor cleaning
    stock_financials = major_share_holders.rename(columns={0: "metric"})
    # send to SQL with SQL Alchemy
    stock_financials.to_sql(f'{stock}_financials', engine, if_exists='replace', index=False)
    print(f'stock financials {stock} data sent to sql')

    # return earnings and send to postgres
    stock_earnings = msft.earnings.reset_index()
    # minor cleaning
    stock_earnings = stock_earnings.rename(str.lower, axis='columns')
    # send to SQL with SQL Alchemy
    stock_earnings.to_sql(f'{stock}_earnings', engine, if_exists='replace', index=False)
    print(f'{stock} earnings data sent to sql')

    # return quarterly earnings and send to postgres
    stock_quarterly_earnings = msft.quarterly_earnings.reset_index()
    # minor cleaning
    stock_quarterly_earnings = stock_quarterly_earnings.rename(str.lower, axis='columns')
    # send to SQL with SQL Alchemy
    stock_quarterly_earnings.to_sql(f'{stock}_quarterly_earnings', engine, if_exists='replace', index=False)
    print(f'{stock} quarterly earnings data sent to sql')

    #return news and send to postgres
    news_list = msft.news
    column_names = ["uuid", "title", "publisher", "link", "provider_publish_time", "type"]
    news_df = pd.DataFrame(columns=column_names)
    news_df['uuid'] = [x['uuid'] for x in news_list]
    news_df['title'] = [x['title'] for x in news_list]
    news_df['publisher'] = [x['publisher'] for x in news_list]
    news_df['link'] = [x['link'] for x in news_list]
    # old way of formatting dates
    # dates = [int(x['providerPublishTime']) for x in news_list]
    # func = lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')
    # news_df['providerPublishTime'] = list(map(func, dates))
    # formatting dates
    dates = [int(x['providerPublishTime']) for x in news_list]
    news_df['provider_publish_time'] = [datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') for x in dates]
    # type
    news_df['type'] = [x['type'] for x in news_list]
    # send to SQL with SQL Alchemy
    news_df.to_sql(f'{stock}_news', engine, if_exists='replace', index=False)
    print(f'{stock} news sent to sql')

    #end of function
    end = time.time()
    print(f'{stock} extracted in {"{:.2f}".format(end-start)} seconds')

# function to extract data for multiple companies for comparison
def companies_returned(companies):
    '''
    Function to apply stock_sql_send to multiple stocks.

    Arguments:
        # companies - takes a list of stocks e.g. ['IAG.L', '0293.HK', 'AF.PA']

    Return:
        # string - indicates that the data has been extracted and sent to SQL.
    '''
    # [stock_sql_send(item) for item in companies]
    list(map(stock_sql_send, companies))
    return print('\n SQL Updated')
